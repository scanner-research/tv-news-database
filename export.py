import psycopg2
import time
import tqdm
from typing import List, Tuple
import os
from schema import Identity, Gender
import sqlalchemy

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


def encode_payload(is_male, is_nonbinary, is_host, height):
    ret = 0
    if is_male:
        ret |= 1
    if is_nonbinary:
        ret |= 1 << 1
    if is_host:
        ret |= 1 << 2
    ret |= height << 3
    return ret


def round_height(height):
    return min(round(height * 31), 31)  # 5-bits

sql = """
-- Get the most confident identity for each face (90 seconds)
WITH identities AS (
	SELECT face_identity.identity_id, face_identity.score, face_identity.face_id
	FROM face_identity
	INNER JOIN (
		SELECT face_id, MAX(score) AS max_score
		FROM face_identity
		WHERE labeler_id = 609 or labeler_id = 610 -- Only rekognition
		GROUP BY face_id
	) AS t
	ON face_identity.face_id = t.face_id AND face_identity.score = t.max_score
	WHERE labeler_id = 609 or labeler_id = 610 -- Only rekognition
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
		WHERE labeler_id = 1 -- manual assignment (nonbinary override)
	) manual_gender
	FULL OUTER JOIN (
		SELECT face_id, gender_id, score
		FROM face_gender
		WHERE labeler_id = 551 -- KNN-gender
	) AS knn_gender
	ON manual_gender.face_id = knn_gender.face_id
	),

-- Get all unique hosts (3 milliseconds)
	hosts AS (
	SELECT DISTINCT identity_id FROM (
		SELECT identity_id FROM channel_host
		UNION ALL 
		SELECT identity_id FROM canonical_show_host) t
	)

-- Join face with gender, identity, hosts, and frames (45 minutes)
SELECT
	face.id as face_id,
	frame.video_id,
	frame.number AS frame_number,
	(face.bbox_y2 - face.bbox_y1) AS height,
	genders.gender_id,
	genders.score AS gender_score,
	identities.identity_id,
	identities.score as identity_score,
	hosts.identity_id IS NOT NULL AS is_host
FROM face
LEFT JOIN identities ON identities.face_id = face.id
LEFT JOIN genders ON genders.face_id = face.id
LEFT JOIN hosts ON hosts.identity_id = identities.identity_id
LEFT JOIN frame on face.frame_id = frame.id
ORDER BY
	frame.video_id,
	identities.identity_id,
	frame.number,
	face.id
"""

start_time = time.time()
password = os.getenv("POSTGRES_PASSWORD")
conn = psycopg2.connect(dbname="tvnews", user="admin", host='localhost', password=password)
cur = conn.cursor()

engine = sqlalchemy.create_engine("postgresql://admin:{}@localhost/tvnews".format(password))
Session = sqlalchemy.orm.sessionmaker(bind=engine)
session = Session()

IDENTITY_INTERVAL_DIR = '/newdisk/intervals'
if not os.path.exists(IDENTITY_INTERVAL_DIR):
    os.makedirs(IDENTITY_INTERVAL_DIR)

identity_ilist_writers = {}
identities = session.query(Identity).all()
identity_id_to_name = {i.id : i.name.lower() for i in identities}

def flush_identity_accumulators(video_id, ilist_accumulators):
    for identity_id, face_ilist in ilist_accumulators.items():
        if face_ilist:
            if identity_id not in identity_ilist_writers:
                identity_ilist_writers[identity_id] = IntervalListMappingWriter(
                    os.path.join(
                        IDENTITY_INTERVAL_DIR, 
                        '{}.ilist.bin'.format(identity_id_to_name[identity_id])
                    ), 1)
            identity_ilist_writers[identity_id].write(video_id, face_ilist)

MALE_GENDER_ID = session.query(Gender).filter_by(name='M').first().id 
NONBINARY_GENDER_ID = session.query(Gender).filter_by(name='U').first().id 
SAMPLE_LENGTH = 3000
n_videos_done = 0
curr_video_id = None
curr_ident_id = None

print("Starting query")
cur.execute(sql)
print("Time to first row: {:.3f} seconds".format(time.time() - start_time))

for row in tqdm.tqdm(cur):

    face_id, video_id, frame_number, height, gender_id, gender_score, identity_id, identity_score, is_host = row
    
    if video_id != curr_video_id:
        if curr_video_id is not None:
            n_videos_done += 1
            if n_videos_done % 1000 == 0:
                print('Processed {} videos in {:.3f} seconds'.format(n_videos_done, time.time() - start_time))
            flush_identity_accumulators(curr_video_id, curr_ilist_accumulators)
                    
        curr_video_id = video_id
        curr_ilist_accumulators = defaultdict(list)
        
    # TODO: remove this once we know the print doesn't trigger and there are no
    # duplicates
    if curr_ident_id is None or curr_ident_id != identity_id:
        curr_face_id = None
    curr_ident_id = identity_id
    if curr_face_id == face_id:
        print("Duplicate face id")
        continue
    curr_face_id = face_id
    start_ms = frame_number * SAMPLE_LENGTH
    end_ms = start_ms + SAMPLE_LENGTH
    curr_ilist_accumulators[identity_id].append(
        (start_ms, end_ms, 
         encode_payload(
             fgender_id == MALE_GENDER_ID, 
             fgender_id == NONBINARY_GENDER_ID, 
             is_host,
             round_height(fheight)
         ))
    )
    
if curr_video_id is not None:
    flush_identity_accumulators(curr_video_id, curr_ilist_accumulators)
            
for iw in identity_ilist_writers.values():
    iw.close()

print("Total time to export: {:3f} seconds".format(time.time() - start_time))
