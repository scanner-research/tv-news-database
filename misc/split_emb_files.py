#!/usr/bin/env python3

import argparse
import os
import sys
from datetime import datetime
import numpy as np
from tqdm import tqdm

from rs_embed import EmbeddingData

sys.path.append('../')
import schema
import util


EMBEDDING_DIM = 128


def get_face_ids(session, frame_sampler, video):
    face_ids = session.query(schema.Face.id).join(schema.Frame).filter(
        schema.Frame.video_id == video.id
    ).filter(
        schema.Frame.sampler_id == frame_sampler.id
    ).all()
    face_ids = [x[0] for x in face_ids]
    print('Found {} face ids for: {}'.format(len(face_ids), video.name))
    return face_ids


def write_emb_files(emb_data, out_dir, video, face_ids):
    ids_and_embs = sorted(emb_data.get(face_ids))
    sorted_ids, sorted_embs = zip(*ids_and_embs)
    if len(sorted_ids) == 0:
        return
    if len(face_ids) != len(sorted_ids):
        print('Expected {} ids, found {}: {}'.format(
            len(face_ids), len(sorted_ids), video.name))

    emb_path = os.path.join(out_dir, '{}.npz'.format(video.name))
    np.savez_compressed(
        emb_path, ids=np.array(sorted_ids, dtype=np.int64),
        data=np.array(sorted_embs, dtype=np.float32))


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('in_ids', type=str)
    parser.add_argument('in_embs', type=str)
    parser.add_argument('out_dir', type=str)
    parser.add_argument('--db-name', type=str, default='tvnews')
    parser.add_argument('--db-user', type=str, default='admin')
    return parser.parse_args()


def main(in_ids, in_embs, out_dir, db_name, db_user):
    password = os.getenv('POSTGRES_PASSWORD')
    session = util.get_db_session(db_user, password, db_name)

    # Only repackage embeddings for the old 3s data.
    frame_sampler = session.query(schema.FrameSampler).filter_by(
        name='3s'
    ).one()

    videos = list(session.query(schema.Video).filter(
        # TODO: we only want to repackage the embeddings for pre-2019
        schema.Video.time < datetime(2019, 1, 1)
    ).all())

    emb_data = EmbeddingData(in_ids, in_embs, EMBEDDING_DIM)

    os.makedirs(out_dir, exist_ok=True)
    for video in tqdm(videos):
        face_ids = get_face_ids(session, frame_sampler, video)
        if len(face_ids) > 0:
            write_emb_files(emb_data, out_dir, video, face_ids)
    print('Done!')

if __name__ == '__main__':
    main(**vars(get_args()))
