#!/usr/bin/env python3
"""
Script to backfill identities using Amazon's labels
"""

import argparse
import os
from multiprocessing import Pool
import psycopg2
import numpy as np
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import precision_score
from tqdm import tqdm

import schema
from util import get_db_session


MIN_SAMPLE_YEAR = 2019
N_POS_SAMPLES = 500
N_NEG_SAMPLES = 5000
PRED_THRESHOLD = 0.95
K = 21


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('person_name', type=str,
                        help='Name of person to backfill')
    parser.add_argument('face_emb_dir', type=str,
                        help='Directory to load embeddings from')
    parser.add_argument('--db-name', type=str, default='tvnews')
    parser.add_argument('--db-user', type=str, default='admin')
    return parser.parse_args()


def load_embs(fname):
    tmp = np.load(fname)
    return tmp['ids'], tmp['data']


def sample_pos_face_ids(conn, identity_id, identity_labeler_id,
                        frame_sampler_id):
    cur = conn.cursor()
    sql = """
        SELECT face.id FROM face_identity
        JOIN face ON face.id = face_id
        JOIN frame ON frame.id = frame_id
        JOIN video ON video.id = video_id
        WHERE (
        	DATE_PART('year', video.time) >= {year} AND
        	frame.sampler_id = {frame_sampler} AND
        	face_identity.labeler_id = {identity_labeler} AND
        	face_identity.identity_id = {identity}
        ) ORDER BY random() LIMIT {n}
    """.format(
        year=MIN_SAMPLE_YEAR, frame_sampler=frame_sampler_id,
        identity_labeler=identity_labeler_id,
        identity=identity_id, n=N_POS_SAMPLES)
    cur.execute(sql)
    return set(x[0] for x in cur)


def sample_neg_face_ids(conn, identity_id, frame_sampler_id):
    cur = conn.cursor()
    sql = """
        SELECT face.id FROM face
        LEFT JOIN face_identity ON face.id = face_identity.face_id
        JOIN frame ON frame.id = frame_id
        JOIN video ON video.id = video_id
        WHERE (
        	DATE_PART('year', video.time) >= {year} AND
        	frame.sampler_id = {frame_sampler} AND
        	face_identity.identity_id != {identity}
        ) ORDER BY random() LIMIT {n}
    """.format(
        year=MIN_SAMPLE_YEAR, frame_sampler=frame_sampler_id,
        identity=identity_id, n=N_NEG_SAMPLES)
    cur.execute(sql)
    return set(x[0] for x in cur)


def collect_train_data(face_emb_dir, videos, pos_ids, neg_ids):
    pos_data = []
    neg_data = []
    for v in tqdm(videos, desc='Collecting training data'):
        emb_path = os.path.join(face_emb_dir, v.name + '.npz')
        try:
            ids, embs = load_embs(emb_path)
            for i, face_id in enumerate(ids):
                if face_id in pos_ids:
                    pos_data.append((face_id, embs[i, :]))
                if face_id in neg_ids:
                    neg_data.append((face_id, embs[i, :]))
        except Exception as e:
            print('Failed to load data:', v.name, e)
    return pos_data, neg_data


def predict_for_video(face_emb_dir, video, clf):
    results = []
    emb_path = os.path.join(face_emb_dir, video.name + '.npz')
    try:
        ids, embs = load_embs(emb_path)
        pred = clf.predict_proba(embs)[:, 1]
        for face_id, pred_prob in zip(ids, pred):
            if pred_prob >= PRED_THRESHOLD:
                # Encode the scores as < 0.5 due to conflict breaking in the DB
                results.append((face_id.item(), pred_prob.item() - 0.5))
    except Exception as e:
        print('Failed to load data:', video.name, e)
    return results


WORKER_INIT_ARGS = None


def predict_for_video_wrapper(v):
    face_emb_dir, clf = WORKER_INIT_ARGS
    return predict_for_video(face_emb_dir, v, clf)


def main(person_name, face_emb_dir, db_name, db_user):
    password = os.getenv('POSTGRES_PASSWORD')
    session = get_db_session(db_user, password, db_name)
    conn = psycopg2.connect(dbname=db_name, user=db_user,
                            host='localhost', password=password)

    identity_object = session.query(schema.Identity).filter_by(
        name=person_name
    ).one()
    frame_sampler_object = session.query(schema.FrameSampler).filter_by(
        name='1s'
    ).one()
    identity_labeler_object = session.query(schema.Labeler).filter_by(
        name='face-identity-rekognition'
    ).one()
    backfill_labeler_object = session.query(schema.Labeler).filter_by(
        name='face-identity-rekognition:backfill-{}'.format(
            person_name.replace(' ', '_'))
    ).one()

    videos = list(session.query(schema.Video).all())
    sample_videos = [v for v in videos if v.time.year >= MIN_SAMPLE_YEAR]
    pred_videos = [v for v in videos if v.time.year < MIN_SAMPLE_YEAR]
    print('Sampling from {} videos'.format(len(sample_videos)))

    pos_face_ids = sample_pos_face_ids(
        conn, identity_object.id, identity_labeler_object.id,
        frame_sampler_object.id)
    neg_face_ids = sample_neg_face_ids(conn, identity_object.id,
        frame_sampler_object.id)
    print('Sampled {} positive and {} negative examples'.format(
        len(pos_face_ids), len(neg_face_ids)))

    pos_data, neg_data = collect_train_data(
        face_emb_dir, sample_videos, pos_face_ids, neg_face_ids)
    print('Collected {} positive and {} negative examples'.format(
        len(pos_data), len(neg_data)))

    X_pos = [x[1] for x in pos_data]
    X_neg = [x[1] for x in neg_data]
    X_all = np.stack([*X_pos, *X_neg])
    y_all = np.zeros(X_all.shape[0])
    y_all[:len(X_pos)] = 1

    X_train, X_test, y_train, y_test = train_test_split(
        X_all, y_all, test_size=0.1, shuffle=True)

    clf = KNeighborsClassifier(n_neighbors=K)
    clf.fit(X_train, y_train)
    test_acc = clf.score(X_test, y_test)
    print('Test accuracy:', test_acc)
    assert test_acc >= 0.95, 'Test accuracy is too low: {}'.format(test_acc)
    test_prec = precision_score(y_test, clf.predict(X_test))
    print('Test precision:', test_prec)

    global WORKER_INIT_ARGS
    WORKER_INIT_ARGS = face_emb_dir, clf

    new_label_count = 0
    with Pool() as p:
        for pred in tqdm(
                p.imap_unordered(predict_for_video_wrapper, pred_videos),
                desc='Running predictions', total=len(pred_videos)
        ):
            new_label_count += len(pred)
            for face_id, score in pred:
                face_ident = schema.FaceIdentity(
                    face_id=face_id, identity_id=identity_object.id,
                    labeler_id=backfill_labeler_object.id,
                    score=score)
                session.add(face_ident)

    print('Predicted {} new faces'.format(new_label_count))
    session.commit()
    print('Done!')


if __name__ == '__main__':
    main(**vars(get_args()))
