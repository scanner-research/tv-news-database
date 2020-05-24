"""
This script exports data from the postgres source of truth database in a format
the tv-news-viewer can use. This includes the following files:

videos.json
faces.ilist.bin
commercials.iset.bin
people/<each identity with substantial screen time>.ilist.bin
face-bboxes/<each video>.json

This script opens a very large number of file descriptors (40k or so), so it may require
you to raise the system and/or per process limit. You can edit /etc/security/limits.conf
to change the per process limit.

It runs in approximately 2 hours.
"""

import psycopg2
import time
import tqdm
from typing import List, Tuple
import os
from sqlalchemy import func
from collections import defaultdict
import json

import schema
from util import get_db_session

# Adapted from https://github.com/scanner-research/rs-intervalset/blob/master/rs_intervalset/writer.py
class IntervalListMappingWriter(object):

    def __init__(self, path: str, payload_len: int):
        self._fp = open(path, 'wb')
        self._path = path
        self._payload_len = payload_len

    def __enter__(self) -> 'IntervalListMappingWriter':
        return self

    def __exit__(self, type, value, tb) -> None:
        self.close()

    def __fmt_u32(self, v: int) -> bytes:
        return v.to_bytes(4, byteorder='little')

    def __fmt_payload(self, v: int) -> bytes:
        return v.to_bytes(self._payload_len, byteorder='little')

    def write(self, id_: int, intervals: List[Tuple[int, int, int]]) -> None:
        assert self._fp is not None
        self._fp.write(self.__fmt_u32(id_))
        self._fp.write(self.__fmt_u32(len(intervals)))
        for a, b, c in intervals:
            assert b > a, 'invalid interval: ({}, {})'.format(a, b)
            self._fp.write(self.__fmt_u32(a))
            self._fp.write(self.__fmt_u32(b))
            self._fp.write(self.__fmt_payload(c))

    def close(self) -> None:
        if self._fp is not None:
            self._fp.close()
            self._fp = None

class IntervalSetMappingWriter(object):

    def __init__(self, path: str):
        self._fp = open(path, 'wb')
        self._path = path

    def __enter__(self) -> 'IntervalSetMappingWriter':
        return self

    def __exit__(self, type, value, tb):
        self.close()

    def __fmt_u32(self, v: int) -> bytes:
        return v.to_bytes(4, byteorder='little')

    def write(self, id_: int, intervals: List[Tuple[int, int]]) -> None:
        assert self._fp is not None
        self._fp.write(self.__fmt_u32(id_))
        self._fp.write(self.__fmt_u32(len(intervals)))
        for a, b in intervals:
            assert b > a, 'invalid interval: ({}, {})'.format(a, b)
            self._fp.write(self.__fmt_u32(a))
            self._fp.write(self.__fmt_u32(b))

    def close(self) -> None:
        if self._fp is not None:
            self._fp.close()
            self._fp = None

def encode_payload(is_male, is_nonbinary, is_host, height):
    ret = 0
    if is_male:
        ret |= 1
    if is_nonbinary:
        ret |= 1 << 1
    if is_host:
        ret |= 1 << 2
    ret |= min(round(height * 31), 31) << 3 # 5-bits
    return ret

def get_labeler(name):
    return session.query(table.Labeler).filter_by(name=name).one()

def get_frame_sampler(name):
    return session.query(table.FrameSampler).filter_by(name=name).one()

def get_gender(name):
    return session.query(table.Gender).filter_by(name=name).one()

# Given a connection to a database, get all identity ids and names with at
# least 15 minutes of screentime. Takes about 3 minutes.
def get_selected_identities(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT identity.id, identity.name
        FROM face_identity
        INNER JOIN identity ON identity.id = face_identity.identity_id
        GROUP BY identity.id, identity.name
        HAVING COUNT(*) > 15 * 60 / 3 -- at a sampling rate of once every 3 seconds, this is 15 minutes
    """)
    return cur.fetchall()


def get_all_identities(conn):
    cur = conn.cursor()
    cur.execute('SELECT identity.id, identity.name FROM identity')
    return cur.fetchall()


def export_commercials(conn):
    start_time = time.time()
    COMMERCIAL_INTERVAL_FILE = os.path.join(WIDGET_DATA_DIR, 'commercials.iset.bin')

    comm_labeler = get_labeler('haotian-commercials')

    # By default, psychopg2 loads the entire dataset in memory. Specifying a name
    # for the cursor makes it a server side cursor, which fetches data in chunks.
    cur = conn.cursor(name="commercial_cursor")
    # This query should run in about 6 seconds
    print("Starting commercial export")
    cur.execute("""
        SELECT
            video_id,
            min_frame / fps * 1000 as start_ms,
            max_frame / fps * 1000 AS end_ms
        FROM commercial
        LEFT JOIN video ON commercial.video_id = video.id
        WHERE commercial.labeler_id={comm_labeler} AND NOT is_corrupt AND NOT is_duplicate
        ORDER BY video_id, start_ms
    """.format(comm_labeler=comm_labeler.id))

    with IntervalSetMappingWriter(COMMERCIAL_INTERVAL_FILE) as interval_writer:
        cur_video_id = None
        buffered_tuples = []
        for video_id, start_ms, end_ms in cur:
            if video_id != cur_video_id:
                if len(buffered_tuples) > 0:
                    interval_writer.write(cur_video_id, buffered_tuples)
                cur_video_id = video_id
                buffered_tuples = []
            buffered_tuples.append((int(start_ms), int(end_ms)))

        if len(buffered_tuples) > 0:
            interval_writer.write(cur_video_id, buffered_tuples)
    print("Finished commercial export in {:.3f} seconds".format(time.time() - start_time))

# Join faces against just about every other table, and return a cursor for the
# results. It takes about 90 minutes total.
def get_faces_and_identties_from_db(conn):
    aws_identity_labeler = get_labeler('face-identity-rekognition')
    aws_prop_identity_labeler = get_labeler('face-identity-rekognition:l2-dist-thresh=0.7')
    manual_gender_labeler = get_labeler('handlabeled-gender')
    knn_gender_labeler = get_labeler('knn-gender')

    sampler_3s = get_frame_sampler('3s')
    sampler_1s = get_frame_sampler('1s')

    sql = """
    WITH identities AS (
        -- Get the most confident identity for each face (10 minutes)
        SELECT face_identity.identity_id, face_identity.score, face_identity.face_id
        FROM face_identity
        INNER JOIN (
            SELECT face_id, MAX(score) AS max_score
            FROM face_identity
            WHERE labeler_id = {aws_identity} or labeler_id = {prop_identity} -- Only rekognition
            GROUP BY face_id
        ) AS t
        ON face_identity.face_id = t.face_id AND face_identity.score = t.max_score
        WHERE labeler_id = {aws_identity} or labeler_id = {prop_identity} -- Only rekognition
        ),

    -- Get the gender for each face, with manual relabeling taking precedence (5 minutes)
        genders AS (
        SELECT
            CASE WHEN manual_gender.face_id IS NULL
                THEN knn_gender.face_id
                ELSE manual_gender.face_id
            END AS face_id,
            CASE WHEN manual_gender.face_id IS NULL
                THEN knn_gender.gender_id
                ELSE manual_gender.gender_id
            END AS gender_id,
            CASE WHEN manual_gender.face_id IS NULL
                THEN knn_gender.score
                ELSE manual_gender.score
            END AS score
        FROM (
            SELECT face_id, gender_id, score
            FROM face_gender
            WHERE labeler_id = {manual_gender} -- manual assignment (nonbinary override)
        ) manual_gender
        FULL OUTER JOIN (
            SELECT face_id, gender_id, score
            FROM face_gender
            WHERE labeler_id = {knn_gender} -- KNN-gender
        ) AS knn_gender
        ON manual_gender.face_id = knn_gender.face_id
        ),

    -- Join face with gender, identity, hosts, and frames (45 minutes)
    SELECT
        face.id as face_id,
        frame.video_id,
        frame.sampler_id,
        CAST(frame.number * (1000.0 / video.fps) AS INTEGER) AS start_ms,
        genders.gender_id,
        genders.score AS gender_score,
        identities.identity_id,
        identities.score as identity_score,
        (channel_host.identity_id IS NOT NULL or canonical_show_host.identity_id IS NOT NULL) AS is_host,
        face.bbox_x1,
        face.bbox_x2,
        face.bbox_y1,
        face.bbox_y2
    FROM face
    LEFT JOIN identities ON identities.face_id = face.id
    LEFT JOIN genders ON genders.face_id = face.id
    LEFT JOIN frame ON face.frame_id = frame.id
    LEFT JOIN video ON frame.video_id = video.id
    LEFT JOIN show ON video.show_id = show.id
    LEFT JOIN channel_host ON (
        show.channel_id = channel_host.channel_id AND identities.identity_id = channel_host.identity_id
    )
    LEFT JOIN canonical_show_host ON (
        show.canonical_show_id = canonical_show_host.canonical_show_id AND identities.identity_id = canonical_show_host.identity_id
    )
    WHERE NOT video.is_corrupt AND NOT video.is_duplicate AND (
        (year(video.time) >= 2019 AND frame.sampler_id = {sampler_1s}) OR
        (year(video.time) < 2019 AMD frame.sampler_id = {sampler_3s})
    )
    ORDER BY
        frame.video_id,
        frame.number,
        identities.identity_id,
        face.id
    """.format(sampler_1s=sampler_1s.id,
               sampler_3s=sampler_3s.id,
               knn_gender=knn_gender_labeler.id,
               manual_gender=manual_gender_labeler.id,
               aws_identity=aws_identity_labeler.id,
               prop_identity=aws_prop_identity_labeler.id)

    print("Starting the big query")
    # By default, psychopg2 loads the entire dataset in memory. Specifying a name
    # for the cursor makes it a server side cursor, which fetches data in chunks.
    cur = conn.cursor(name="face_cursor")
    cur.execute(sql)
    return cur

# If the query in get_faces_and_identties_from_db has already been run, exporting to a
# CSV file, use this to use that file as a starting point.
def get_identities_from_file(path):
    import csv

    with open(path) as fp:
        fp.readline() # Skip headers
        reader = csv.reader(fp)

        for row in reader:
            face_id, video_id, start_ms, gender_id, gender_score, identity_id, identity_score, is_host, bbox_x1, bbox_x2, bbox_y1, bbox_y2 = row
            face_id = int(face_id)
            video_id = int(video_id)
            start_ms = int(float(start_ms))
            gender_id = int(gender_id) if len(gender_id) > 0 else None
            if len(identity_id) == 0:
                continue
            identity_id = int(identity_id) if len(identity_id) > 0 else None
            identity_score = float(identity_score) if len(identity_score) > 0 else None
            is_host = is_host == 't'
            bbox_x1, bbox_x2 = float(bbox_x1), float(bbox_x2)
            bbox_y1, bbox_y2 = float(bbox_y1), float(bbox_y2)
            yield (face_id, video_id, start_ms, gender_id, gender_score, identity_id, identity_score, is_host, bbox_x1, bbox_x2, bbox_y1, bbox_y2)

def export_faces_and_identities(conn):
    IDENTITY_INTERVAL_DIR = os.path.join(WIDGET_DATA_DIR, 'people')
    os.makedirs(IDENTITY_INTERVAL_DIR, exist_ok=True)

    FACE_BBOX_DIR = os.path.join(WIDGET_DATA_DIR, 'face-bboxes')
    os.makedirs(FACE_BBOX_DIR, exist_ok=True)

    start_time = time.time()
    selected_identities = get_selected_identities(conn)
    print("Fetched {} selected identities in {:.3f} seconds".format(
        len(selected_identities), time.time() - start_time))

    identity_ilist_writers = {
        id : IntervalListMappingWriter(
            os.path.join(IDENTITY_INTERVAL_DIR, '{}.ilist.bin'.format(name.lower())), 1
        ) for id, name in selected_identities
    }

    # Get just the ids and turn it into a set for faster search
    selected_identities = {id for id, name in selected_identities}

    def flush_identity_accumulators(video_id, ilist_accumulators):
        for identity_id, face_ilist in ilist_accumulators.items():
            identity_ilist_writers[identity_id].write(video_id, face_ilist)

    # Lookup identity id to name
    identity_id_to_name = {id: name for id, name in get_all_identities()}

    def save_bboxes_for_video(video_id, faces):
        identity_ids = {f['i'] for f in faces if 'i' in f}
        identities = [(identity_id_to_name.get(i, ''), i) for i in sorted(identity_ids)]
        with open(os.path.join(FACE_BBOX_DIR, '{}.json'.format(video_id))) as fp:
            json.dump({
                'faces': faces,
                'ids': identities
            })

    MALE_GENDER_ID = get_gender('M').id
    NONBINARY_GENDER_ID = get_gender('O').id

    SAMPLER_1S_ID = get_frame_sampler('1s').id
    SAMPLER_3S_ID = get_frame_sampler('3s').id

    curr_video_id = None

    # Count the number of faces for a progress bar estimate
    face_count = session.query(func.count(schema.Faces.id)).scalar()

    with IntervalListMappingWriter(
        os.path.join(WIDGET_DATA_DIR, 'faces.ilist.bin'),
        1
    ) as all_faces_writer:
        face_iterator = get_faces_and_identties_from_db(conn)

        for row in tqdm.tqdm(face_iterator, total=face_count):
            (
                face_id, video_id, frame_sampler_id, start_ms,
                gender_id, gender_score, identity_id, identity_score, is_host,
                bbox_x1, bbox_x2, bbox_y1, bbox_y2
            ) = row

            if video_id != curr_video_id:
                if curr_video_id is not None:
                    flush_identity_accumulators(curr_video_id, curr_ilist_accumulators)
                    if curr_video_intervals:
                        all_faces_writer.write(curr_video_id, curr_video_intervals)
                    if curr_video_faces:
                        save_bboxes_for_video(curr_video_id, curr_video_faces)

                curr_video_id = video_id
                curr_ilist_accumulators = defaultdict(list)
                curr_video_intervals = []
                curr_video_faces = []

            if frame_sampler_id == SAMPLER_1S_ID:
                end_ms = start_ms + 1000
            elif frame_sampler_id == SAMPLER_3S_ID:
                end_ms = start_ms + 3000
            else:
                raise Exception('Unknown frame sampler: {}'.format(frame_sampler_id))

            height = bbox_y2 - bbox_y1
            assert height > 0

            interval_entry = (
                start_ms, end_ms,
                encode_payload(
                     gender_id == MALE_GENDER_ID,
                     gender_id == NONBINARY_GENDER_ID,
                     is_host, height)
            )
            if identity_id in selected_identities:
                curr_ilist_accumulators[identity_id].append(interval_entry)
            curr_video_intervals.append(interval_entry)

            face_meta = {
                'g': 'O' if gender_id == NONBINARY_GENDER_ID else ('M' if gender_id == MALE_GENDER_ID else 'F),
                't': [round(start_ms / 1000, 2), round(end_ms / 1000, 2)],
                'b': [
                    round(bbox_x1, 2), round(bbox_y1, 2),
                    round(bbox_x2, 2), round(bbox_y2, 2)
                ]
            }
            if identity_id is not None:
                face_meta[i] = identity_id
            curr_video_faces.append(face_meta)

        if curr_video_id is not None:
            flush_identity_accumulators(curr_video_id, curr_ilist_accumulators)
            if curr_video_intervals:
                all_faces_writer.write(curr_video_id, curr_video_intervals)
            if curr_video_faces:
                save_bboxes_for_video(curr_video_id, curr_video_faces)

        for iw in identity_ilist_writers.values():
            iw.close()

def export_videos(conn):
    start_time = time.time()
    VIDEO_FILE = os.path.join(WIDGET_DATA_DIR, 'videos.json')

    # By default, psychopg2 loads the entire dataset in memory. Specifying a name
    # for the cursor makes it a server side cursor, which fetches data in chunks.
    cur = conn.cursor(name="video_cursor")
    # This query should run in about 6 seconds
    print("Starting video export")
    cur.execute("""
        SELECT
            video.id,
            SPLIT_PART(video.name, '.mp4', 1),
            canonical_show.name,
            channel.name,
            num_frames,
            fps,
            width,
            height
        FROM video
        LEFT JOIN show ON video.show_id = show.id
        LEFT JOIN canonical_show ON show.canonical_show_id = canonical_show.id
        LEFT JOIN channel ON channel.id=show.channel_id
        WHERE NOT video.is_corrupt AND NOT video.is_duplicate
    """)

    with open(VIDEO_FILE, 'w') as f:
        json.dump(cur.fetchall(), f)

    print("Finished video export in {:.3f} seconds".format(time.time() - start_time))

if __name__ == '__main__':
    start_time = time.time()
    password = os.getenv("POSTGRES_PASSWORD")
    conn = psycopg2.connect(dbname="tvnews", user="admin", host='localhost', password=password)
    session = get_db_session(password)

    WIDGET_DATA_DIR = '/newdisk/widget-data'
    os.makedirs(WIDGET_DATA_DIR, exist_ok=True)

    export_videos(conn)
    export_commercials(conn)
    export_faces_and_identities(conn)

    print("Total time to export: {:3f} seconds".format(time.time() - start_time))
