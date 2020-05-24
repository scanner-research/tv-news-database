#!/usr/bin/env python3

import argparse
import os
import json
import sqlalchemy
import psycopg2
import tqdm
import subprocess
import numpy as np
from typing import Dict, NamedTuple

import schema
from util import get_db_session, get_or_create, parse_video_name


EMBEDDING_DIM = 128


class ImportContext(NamedTuple):
    import_path: str
    face_emb_path: str
    caption_path: str

    frame_sampler: schema.FrameSampler

    commercial_labeler: schema.Labeler
    face_labeler: schema.Labeler
    gender_labeler: schema.Labeler
    aws_identity_labeler: schema.Labeler
    aws_prop_identity_labeler: schema.Labeler

    male_gender: schema.Gender
    female_gender: schema.Gender


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('import_path', type=str,
                        help='Directory with pipeline outputs')
    parser.add_argument('face_emb_path', type=str,
                        help='Directory to save embeddings to')
    parser.add_argument('caption_path', type=str,
                        help='Directory to save captions to')
    parser.add_argument('--ignore-existing-videos', action='store_true',
                        help='Skip videos already in the database')
    parser.add_argument('--tmp-data-dir', type=str,
                        default='/tmp/tv-news-db-staging')
    return parser.parse_args()


def load_json(fpath):
    with open(fpath) as fp:
        return json.load(fp)


def get_import_context(
    session, import_path: str, face_emb_path: str, caption_path: str
):
    frame_sampler_object = session.query(schema.FrameSampler).filter_by(
        name='1s'
    ).one()

    commercial_labeler_object = session.query(schema.Labeler).filter_by(
        name='haotian-commercial'
    ).one()

    gender_labeler_object = session.query(schema.Labeler).filter_by(
        name='knn-gender'
    ).one()
    face_labeler_object = session.query(schema.Labeler).filter_by(
        name='MTCNN'
    ).one()
    aws_identity_labeler_object = session.query(schema.Labeler).filter_by(
        name='face-identity-rekognition'
    ).one()
    aws_prop_identity_labeler_object = session.query(schema.Labeler).filter_by(
        name='face-identity-rekognition:l2-dist-thresh=0.7'
    ).one()

    male_gender_object = session.query(schema.Gender).filter_by(
        name='M'
    ).one()
    female_gender_object = session.query(schema.Gender).filter_by(
        name='F'
    ).one()

    return ImportContext(
        import_path=import_path,
        face_emb_path=face_emb_path,
        caption_path=caption_path,
        frame_sampler=frame_sampler_object,
        commercial_labeler=commercial_labeler_object,
        face_labeler=face_labeler_object,
        gender_labeler=gender_labeler_object,
        aws_identity_labeler=aws_identity_labeler_object,
        aws_prop_identity_labeler=aws_prop_identity_labeler_object,
        male_gender=male_gender_object,
        female_gender=female_gender_object)


def import_video(session, import_context: ImportContext, video_path: str,
                 video_name: str):
    meta_path = os.path.join(video_path, 'metadata.json')
    meta_dict = load_json(meta_path)

    channel, show, timestamp = parse_video_name(video_name)
    show_object = get_or_create_show(session, channel, show)

    video_object = schema.Video(
        name=video_name,
        num_frames=meta_dict['frames'],
        fps=meta_dict['fps'],
        width=meta_dict['width'],
        height=meta_dict['height'],
        time=timestamp,
        show_id=show_object.id,
        is_duplicate=False,
        is_corrupt=False)
    session.add(video_object)
    session.flush() # need video id
    return video_object


def import_commercials(
    session, import_context: ImportContext, video_path: str,
    video_object: schema.Video
):
    commercial_file = os.path.join(video_path, 'commercials.json')
    for start_frame, end_frame in load_json(commercial_file):
        assert start_frame < end_frame, \
            'Invalid commercial: {} - {}'.format(start_frame, end_frame)
        commercial_object = schema.Commerical(
            labeler_id=import_context.commercial_labeler.id,
            max_frame=end_frame, min_frame=start_frame,
            video_id=video_object.id)
        session.add(commercial_object)


def import_faces(
    session, import_context: ImportContext, video_path: str,
    video_object: schema.Video
) -> Dict[int, int]:
    bbox_file = os.path.join(video_path, 'bboxes.json')
    face_id_map = {}
    frame_objects = {}
    for orig_face_id, face_meta in load_json(bbox_file):
        frame_num = face_meta['frame_num']

        frame_object = frame_objects.get(frame_num)
        if frame_object is None:
            frame_object = schema.Frame(
                number=frame_num,
                video_id=video_object.id,
                sampler_id=import_context.frame_sampler.id)
            session.add(frame_object)
            session.flush() # Need frame ids
            frame_objects[frame_num] = frame_object

        face_object = schema.Face(
            bbox_x1=face_meta['bbox']['x1'],
            bbox_x2=face_meta['bbox']['x2'],
            bbox_y1=face_meta['bbox']['y1'],
            bbox_y2=face_meta['bbox']['y2'],
            labeler_id=import_context.face_labeler.id,
            score=face_meta['bbox']['score'],
            frame_id=frame_object.id)
        session.add(face_object)
        session.flush() # Need face ids

        face_id_map[orig_face_id] = face_object.id
    assert len(set(face_id_map.values())) == len(face_id_map)
    return face_id_map


def import_face_genders(
    session, import_context: ImportContext, video_path: str,
    face_id_map: Dict[int, int]
):
    gender_file = os.path.join(video_path, 'gender.json')
    for orig_face_id, gender, score in load_json(gender_file):
        assert score >= 0.5 and score <= 1., \
            'Score has an invalid range: {}'.format(score)

        face_id = face_id_map[orig_face_id].id
        if gender == 'M':
            gender_id = import_context.male_gender.id
        elif gender == 'F':
            gender_id = import_context.female_gender.id
        else:
            raise Exception('Unknown gender: {}'.format(gender))

        face_gender_object = schema.FaceGender(
            face_id=face_id, gender_id=gender_id,
            labeler_id=import_context.gender_labeler.id,
            score=score)
        session.add(face_gender_object)


@lru_cache(1024)
def get_or_create_identity(session, **kwargs):
    instance = session.query(schema.Identity).filter_by(**kwargs).first()
    if not instance:
        print('Creating identity:', kwargs)
        instance = model(is_ignore=False, **kwargs)
        session.add(instance)
        # This flush is necessary in order to populate the primary key
        session.flush()
    return instance


def import_face_identities(
    session, import_context: ImportContext, video_path: str,
    face_id_map: Dict[int, int]
):
    base_identity_file = os.path.join(video_path, 'identities.json')
    prop_identity_file = os.path.join(video_path, 'identities_propagated.json')

    # Add the original AWS identities
    base_face_ids = set()
    for orig_face_id, name, score in load_json(base_face_ids):
        base_face_ids.add(orig_face_id)
        face_id = face_id_map[orig_face_id]
        # TODO: Not sure how this handles null new columns
        identity_object = get_or_create_identity(session, name=name.lower())
        face_identity_object = schema.FaceIdentity(
            face_id=face_id, labeler_id=import_context.aws_identity_labeler.id,
            score=score, identity_id=identity_object.id)
        session.add(face_identity_object)

    # Add the propagated AWS identities
    for orig_face_id, name, score in load_json(prop_identity_file):
        if orig_face_id in base_face_ids:
            continue
        face_id = face_id_map[orig_face_id]
        identity_object = get_or_create_identity(session, name=name.lower())
        face_identity_object = schema.FaceIdentity(
            face_id=face_id,
            labeler_id=import_context.aws_prop_identity_labeler.id,
            score=score, identity_id=identity_object.id)
            session.add(face_identity_object)


def save_embeddings(import_context, video_object, face_id_map):
    emb_file = os.path.join(video_path, 'embeddings.json')
    face_id_to_emb = {
        face_id_map[orig_face_id]: emb
        for orig_face_id, emb in load_json(emb_file)
    }
    for emb in face_id_to_emb.values():
        assert len(emb) == EMBEDDING_DIM, \
            'Incorrect embedding dim: {} != {}'.format(len(emb), EMBEDDING_DIM)

    id_path = os.path.join(
        import_context.face_emb_path, '{}.ids.npy'.format(video_object.id))
    emb_path = os.path.join(
        import_context.face_emb_path, '{}.data.npy'.format(video_object.id))
    sorted_ids = [i for i in sorted(face_id_to_emb)]
    np.save(id_path, np.array(sorted_ids, dtype=np.int64))
    np.save(emb_path, np.array([face_id_to_emb[i] for i in sorted_ids],
                               dtype=np.float32))


def save_captions(import_context, video_name):
    caption_out_path = os.path.join(
        import_context.caption_path, '{}.align.srt'.format(video_name))
    caption_path = os.path.join(import_context.import_path, 'captions.srt')
    shutil.copyfile(caption_path, caption_out_path)


@lru_cache(1024)
def get_or_create_show(session, channel: str, show: str):
    channel_object = session.query(schema.Channel).filter_by(
        channel=channel
    ).one()
    assert channel_object, 'Unknown channel: {}'.format(channel)

    show_object = session.query(schema.Show).filter_by(
        channel_id=channel_object.id, name=show
    ).first()
    if show_object:
        return show_object
    else:
        canonical_show_object = schema.CanonicalShow(
            name=show, is_recurring=False, channel_id=channel_object.id)
        session.add(canonical_show_object)
        session.flush() # need canonical show id

        show_object = schema.Show(
            name=show, canonical_show_id=canonical_show_object.id,
            channel_id=channel_object.id)
        session.add(show_object)
        session.flush() # need show id
        return show_object


def process_video(
    session, import_context: ImportContext, video_name: str,
    ignore_existing_videos: bool
):
    video_path = os.path.join(import_context.import_path, video_name)
    if not os.path.isdir(video_path):
        print('{} is not a directory'.format(video_path))
        return

    video_object = session.query(schema.Video).filter_by(
        name=video_name
    ).first()
    if video_object:
        if ignore_existing_videos:
            print('Video: {} is already in the database'.format(video_name))
            return
    else:
        print('Importing video: {}'.format(video_name))
        video_object = import_video(session, import_context, video_path,
                                    video_name)

    face_id_map = import_faces(session, import_context, video_path,
                               video_object)
    import_face_genders(session, import_context, video_path, face_id_map)
    import_face_identities(session, import_context, video_path, face_id_map)
    import_commercials(session, import_context, video_path, video_object)
    session.flush()

    save_embeddings(import_context, video_object, face_id_map)
    save_captions(import_context, video_name)


def download_from_gcs(gcs_path, download_path):
    subprocess.check_call([
        'gsutil', '-m', 'cp', '-nr', os.path.join(gcs_path), download_path
    ])
    print('Downloaded data for {} videos.'.format(
          len(os.listdir(download_path))))


def main(import_path, face_emb_path, caption_path, tmp_data_dir,
         ignore_existing_videos):
    password = os.getenv('POSTGRES_PASSWORD')
    session = get_db_session(password)

    assert os.path.isdir(face_emb_path), \
        'Face emb path does not exist! {}'.format(face_emb_path)
    assert os.path.isdir(caption_path), \
        'Caption path does not exist! {}'.format(caption_path)

    if import_path.startswith('gs://'):
        download_from_gcs(import_path, tmp_data_dir)
        print('Saved files from GCS:', tmp_data_dir)
        import_path = tmp_data_dir

    import_context = get_import_context(session, import_path, face_emb_path,
                                        caption_path)
    for video_name in tqdm(sorted(os.listdir(import_path))):
        process_video(session, import_context, video_name,
                      ignore_existing_videos)
    session.commit()
    print('Done!')


if __name__ == '__main__':
    main(**vars(get_args()))
