#!/usr/bin/env python3

import os
import argparse
import json
import time
import sqlalchemy
from sqlalchemy.sql.expression import func

import util
import schema


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('out_file')
    parser.add_argument('-n', type=int, required=True)
    parser.add_argument('-nr', '--no-random', action='store_true')
    parser.add_argument('--year', type=int)
    parser.add_argument('--channel', type=str)
    parser.add_argument('--video', type=str)
    parser.add_argument('--db-name', type=str, default='tvnews')
    parser.add_argument('--db-user', type=str, default='admin')
    return parser.parse_args()


def get_year_from_video_name(video_name):
    yyyymmdd = video_name.split('_')[1]
    return int(yyyymmdd[:4])


def main(out_file, n, no_random, year, channel, video, db_name, db_user):
    password = os.getenv('POSTGRES_PASSWORD')
    session = util.get_db_session(db_user, password, db_name)

    if video is not None:
        year = get_year_from_video_name(video)

    # Only repackage embeddings for the old 3s data.
    frame_sampler = session.query(schema.FrameSampler).filter_by(
        name='3s' if year < 2019 else '1s'
    ).one()

    gender_labeler = session.query(schema.Labeler).filter_by(
        name='knn-gender'
    ).one()

    identity_labeler_ids = [
        l.id for l in session.query(schema.Labeler).filter(
            schema.Labeler.name.like('face-identity-rekognition%')
        ).all()]

    sample_query = session.query(schema.FaceGender).join(schema.Gender).join(
        schema.Face).join(schema.Frame).join(schema.Video)
    if year is not None:
        sample_query = sample_query.filter(
            sqlalchemy.extract('year', schema.Video.time) == year)
    if video is not None:
        sample_query = sample_query.filter(schema.Video.name == video)
    if channel is not None:
        channel_obj = session.query(schema.Channel).filter_by(
            name=channel
        ).one()
        sample_query = sample_query.join(
            schema.Show).filter(schema.Show.channel_id == channel_obj.id)
    sample_query = sample_query.filter(
        schema.FaceGender.labeler_id == gender_labeler.id
    ).filter(
        schema.Frame.sampler_id == frame_sampler.id
    )
    if not no_random:
        sample_query = sample_query.order_by(func.random())
    sample_query = sample_query.limit(n)
    print('Executing query:')
    print(sample_query.statement)
    print()

    start_time = time.time()
    result = []
    for v in sample_query.values(
            'video.name', 'video.extension', 'frame.number', 'face.id',
            'face.bbox_x1', 'face.bbox_x2', 'face.bbox_y1', 'face.bbox_y2',
            'gender.name', 'face_gender.score'
    ):
        (
            video_name, video_ext, frame_num, face_id,
            x1, x2, y1, y2,
            gender_name, gender_score
        ) = v

        identities = list(session.query(schema.FaceIdentity).join(
            schema.Identity
        ).filter(
            schema.FaceIdentity.face_id == face_id
        ).filter(
            schema.FaceIdentity.labeler_id.in_(identity_labeler_ids)
        ).order_by(
            schema.FaceIdentity.score.desc()
        ).values('identity.name', 'face_identity.score'))
        if len(identities) >= 1:
            if len(identities) > 1:
                print('WARNING: multiple Amazon identity labels on one face!')
                print(identities)
            identity, identity_score = identities[0]
        else:
            identity, identity_score = None, 0

        # To get a frameserver url:
        # http://<frameserver>:7500/fetch?path=tvnews/videos/<video>&frame=<frame>
        result.append({
            'video': video_name + video_ext,
            'frame': frame_num,
            'face_id': face_id,
            'bbox': [x1, y1, x2, y2],
            'gender': gender_name,
            'gender_score': gender_score,
            'identity': identity,
            'identity_score': identity_score,
        })
    print('Query finished in {} seconds.'.format(time.time() - start_time))
    with open(out_file, 'w') as fp:
        json.dump(result, fp)
    print('Saved result to:', out_file)


if __name__ == '__main__':
    main(**vars(get_args()))
