#!/usr/bin/env python3

import os
import argparse
import sqlalchemy
import psycopg2
from datetime import datetime

import rs_embed import EmbeddingData
from ... import schema
from ... import util


IN_ID_PATH = 'ids.bin'
IN_EMB_PATH = 'data.bin'
OUT_EMB_PATH = '../emb_dir'


def get_videos():
    # TODO: we only want to repackage the embeddings for pre-2019
    videos = session.query(schema.Video).filter(
        schema.Video.time < datetime(2019, 1, 1)
    ).all()
    for v in videos:
        print(v.name)
    return videos


def get_face_ids(frame_sampler, video):
    face_ids = session.query(schema.Face.id).join(schema.Frame).filter(
        schema.Frame.video_id == video.id
    ).filter(
        schema.Frame.sampler_id == frame_sampler.id
    ).all()
    for face_id in face_ids:
        print(face_id)
    return list(face_ids)


def write_emb_files(out_dir, video, face_ids):
    ids_and_embs = sorted(emb_data.get(face_ids))
    sorted_ids, sorted_embs = zip(*ids_and_embs)
    if len(sorted_ids) == 0:
        return
    if len(face_ids) != len(sorted_ids):
        print('Expected {} ids, found {}: {}'.format(
              len(face_ids), len(sorted_ids), video.name))

    id_path = os.path.join(
        out_dir, '{}.ids.npy'.format(video_object.id))
    emb_path = os.path.join(
        out_dir, '{}.data.npy'.format(video_object.id))
    np.save(id_path, np.array(sorted_ids, dtype=np.int64))
    np.save(emb_path, np.array(sorted_embs, dtype=np.float32))


if __name__ == '__main__':
    password = os.getenv('POSTGRES_PASSWORD')
    session = util.get_db_session(password)

    frame_sampler = session.query(schema.FrameSampler).filter_by(
        name='3s'
    ).first()

    emb_data = EmbeddingData(IN_ID_PATH, IN_EMB_PATH)

    videos = get_videos()
    for video in tqdm(videos):
        face_ids = get_face_ids(frame_sampler, videos)
        if len(face_ids) > 0:
            write_emb_files(OUT_EMB_PATH, face_ids, video)
