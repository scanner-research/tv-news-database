#!/usr/bin/env python3

import argparse
import json
import os
import shutil
import subprocess
from collections import Counter
from functools import lru_cache
from typing import Dict, NamedTuple
import numpy as np
from tqdm import tqdm

import schema
from util import get_db_session, parse_video_name


EMBEDDING_DIM = 128


class ImportContext(NamedTuple):
    face_emb_path: str
    align_caption_path: str
    orig_caption_path: str

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
    parser.add_argument('align_caption_path', type=str,
                        help='Directory to save aligned captions to')
    parser.add_argument('orig_caption_path', type=str,
                        help='Directory to save original captions to')
    parser.add_argument('--import-existing-videos', action='store_true',
                        help='Import videos already in the database')
    parser.add_argument('--tmp-data-dir', type=str,
                        default='/tmp')
    parser.add_argument('--db-name', type=str, default='tvnews')
    parser.add_argument('--db-user', type=str, default='admin')
    return parser.parse_args()


def load_json(fpath: str):
    with open(fpath) as fp:
        return json.load(fp)


def get_import_context(
    session, face_emb_path: str, align_caption_path: str,
    orig_caption_path: str
):
    frame_sampler_object = session.query(schema.FrameSampler).filter_by(
        name='1s'
    ).one()

    commercial_labeler_object = session.query(schema.Labeler).filter_by(
        name='commercials-1s'
    ).one()

    gender_labeler_object = session.query(schema.Labeler).filter_by(
        name='knn-gender'
    ).one()
    face_labeler_object = session.query(schema.Labeler).filter_by(
        name='mtcnn'
    ).one()
    aws_identity_labeler_object = session.query(schema.Labeler).filter_by(
        name='face-identity-rekognition'
    ).one()
    aws_prop_identity_labeler_object = session.query(schema.Labeler).filter_by(
        name='face-identity-rekognition:augmented-l2-dist=0.7'
    ).one()

    male_gender_object = session.query(schema.Gender).filter_by(
        name='M'
    ).one()
    female_gender_object = session.query(schema.Gender).filter_by(
        name='F'
    ).one()

    return ImportContext(
        face_emb_path=face_emb_path,
        align_caption_path=align_caption_path,
        orig_caption_path=orig_caption_path,
        frame_sampler=frame_sampler_object,
        commercial_labeler=commercial_labeler_object,
        face_labeler=face_labeler_object,
        gender_labeler=gender_labeler_object,
        aws_identity_labeler=aws_identity_labeler_object,
        aws_prop_identity_labeler=aws_prop_identity_labeler_object,
        male_gender=male_gender_object,
        female_gender=female_gender_object)


def import_video(session, video_path: str,
                 video_name: str):
    meta_path = os.path.join(video_path, 'metadata.json')
    meta_dict = load_json(meta_path)

    channel, show, timestamp = parse_video_name(video_name)
    show_object = get_or_create_show(session, channel, show)

    video_object = schema.Video(
        name=video_name,
        extension='.mp4',
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
        commercial_object = schema.Commercial(
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
    gender_file = os.path.join(video_path, 'genders.json')
    for orig_face_id, gender, score in load_json(gender_file):
        assert score >= 0.5 and score <= 1., \
            'Score has an invalid range: {}'.format(score)

        face_id = face_id_map[orig_face_id]
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
        instance = schema.Identity(is_ignore=False, **kwargs)
        session.add(instance)
        # This flush is necessary in order to populate the primary key
        session.flush()
    return instance


def import_face_identities(
    session, import_context: ImportContext, video_path: str,
    face_id_map: Dict[int, int]
):
    base_identity_file = os.path.join(video_path, 'identities.json')
    prop_identity_file = os.path.join(video_path, 'identities_propogated.json')

    # Add the original AWS identities
    base_face_ids = set()
    name_to_count = Counter()
    for orig_face_id, name, score in load_json(base_identity_file):
        assert orig_face_id not in base_face_ids
        base_face_ids.add(orig_face_id)
        face_id = face_id_map[orig_face_id]
        lower_name = name.lower()
        name_to_count[lower_name] += 1
        identity_object = get_or_create_identity(session, name=lower_name)
        face_identity_object = schema.FaceIdentity(
            face_id=face_id, labeler_id=import_context.aws_identity_labeler.id,
            score=score, identity_id=identity_object.id)
        session.add(face_identity_object)

    # Collect conflicting votes
    orig_face_id_to_entries = {}
    for orig_face_id, name, score in load_json(prop_identity_file):
        if orig_face_id in base_face_ids:
            continue
        lower_name = name.lower()
        lower_name_count = name_to_count[lower_name]
        if (
            not orig_face_id in orig_face_id_to_entries
            or orig_face_id_to_entries[orig_face_id][-1] < lower_name_count
        ):
            orig_face_id_to_entries[orig_face_id] = (
                lower_name, score, lower_name_count)

    # Add the propagated AWS identities
    for orig_face_id, (lower_name, score, _) in sorted(orig_face_id_to_entries.items()):
        face_id = face_id_map[orig_face_id]
        identity_object = get_or_create_identity(session, name=lower_name)
        face_identity_object = schema.FaceIdentity(
            face_id=face_id,
            labeler_id=import_context.aws_prop_identity_labeler.id,
            score=score, identity_id=identity_object.id)
        session.add(face_identity_object)


def save_embeddings(import_context, video_path, video_name, face_id_map):
    emb_file = os.path.join(video_path, 'embeddings.json')
    face_id_to_emb = {
        face_id_map[orig_face_id]: emb
        for orig_face_id, emb in load_json(emb_file)
    }
    for emb in face_id_to_emb.values():
        assert len(emb) == EMBEDDING_DIM, \
            'Incorrect embedding dim: {} != {}'.format(len(emb), EMBEDDING_DIM)

    emb_path = os.path.join(
        import_context.face_emb_path, '{}.npz'.format(video_name))
    sorted_ids = list(sorted(face_id_to_emb))
    np.savez_compressed(
        emb_path, ids=np.array(sorted_ids, dtype=np.int64),
        data=np.array([face_id_to_emb[i] for i in sorted_ids],
                      dtype=np.float32))


def save_captions(import_context, video_path, video_name):
    align_caption_out_path = os.path.join(
        import_context.align_caption_path, '{}.srt'.format(video_name))
    align_caption_path = os.path.join(video_path, 'captions.srt')
    shutil.copyfile(align_caption_path, align_caption_out_path)

    orig_caption_out_path = os.path.join(
        import_context.orig_caption_path, '{}.srt'.format(video_name))
    orig_caption_path = os.path.join(video_path, 'captions_orig.srt')
    shutil.copyfile(orig_caption_path, orig_caption_out_path)


@lru_cache(1024)
def get_or_create_show(session, channel: str, show: str):
    channel_object = session.query(schema.Channel).filter_by(
        name=channel
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


TAR_GZ_EXT = '.tar.gz'


def process_video(
    session, import_context: ImportContext, video_path: str, video_name: str,
    import_existing_videos: bool
):
    video_object = session.query(schema.Video).filter_by(
        name=video_name
    ).first()
    if video_object:
        if not import_existing_videos:
            print('Video: {} is already in the database'.format(video_name))
            return
    else:
        print('Importing video: {}'.format(video_name))
        video_object = import_video(session, video_path, video_name)

    face_id_map = import_faces(session, import_context, video_path,
                               video_object)
    import_face_genders(session, import_context, video_path, face_id_map)
    import_face_identities(session, import_context, video_path, face_id_map)
    import_commercials(session, import_context, video_path, video_object)
    session.flush()

    save_embeddings(import_context, video_path, video_name, face_id_map)
    save_captions(import_context, video_path, video_name)


# Note: do not make this multiprocessed, it will prevent the transactional
# commit structure
def main(import_path, face_emb_path, align_caption_path, orig_caption_path,
         tmp_data_dir, import_existing_videos, db_name, db_user):
    password = os.getenv('POSTGRES_PASSWORD')
    session = get_db_session(db_user, password, db_name)

    assert os.path.isdir(face_emb_path), \
        'Face emb path does not exist! {}'.format(face_emb_path)
    assert os.path.isdir(align_caption_path), \
        'Align caption path does not exist! {}'.format(align_caption_path)
    assert os.path.isdir(orig_caption_path), \
        'Raw caption path does not exist! {}'.format(orig_caption_path)

    import_context = get_import_context(
        session, face_emb_path, align_caption_path, orig_caption_path)
    for video_name in tqdm(sorted(os.listdir(import_path))):
        if video_name.endswith(TAR_GZ_EXT):
            archive_path = os.path.join(import_path, video_name)
            os.makedirs(tmp_data_dir, exist_ok=True)
            subprocess.check_call(['tar', '-xzf', archive_path, '-C', tmp_data_dir])
            video_name = video_name[:-len(TAR_GZ_EXT)]
            video_path = os.path.join(tmp_data_dir, video_name)
            process_video(session, import_context, video_path,
                          video_name, import_existing_videos)
            shutil.rmtree(video_path)
        else:
            video_path = os.path.join(import_path, video_name)
            if not os.path.isdir(video_path):
                print('{} is not a directory'.format(video_path))
                continue
            process_video(session, import_context, video_path, video_name,
                          import_existing_videos)
    session.commit()
    print('Done!')


if __name__ == '__main__':
    main(**vars(get_args()))
