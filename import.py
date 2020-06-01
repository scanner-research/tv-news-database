#!/usr/bin/env python3

# Import data from the old tvnews database. This takes a very long time...
#
# Don't forget to set postgres performance flags
#
# shared_buffers = 1GB
# work_mem = 2GB
# max_worker_processes = 24
# max_parallel_workers_per_gather = 12
# max_parallel_workers = 12


import argparse
import csv
import os
import shutil
import tempfile
from multiprocessing import Pool
import psycopg2
import sqlalchemy
from tqdm import tqdm

import schema
from util import get_or_create, parse_video_name


def load_videos(session, video_csv, show_to_canonical_show_csv):
    print('Importing video, show, canonical_show')
    canonical_show_dict = {}
    with open(show_to_canonical_show_csv) as fp:
        fp.readline() # Skip headers
        reader = csv.reader(fp)
        for row in reader:
            show, canonical_show, is_recurring = row
            is_recurring = is_recurring.upper() == 'TRUE'
            canonical_show_dict[show] = (canonical_show, is_recurring)

    with open(video_csv) as fp:
        fp.readline() # Skip headers
        reader = csv.reader(fp)
        for row in tqdm(reader):
            (
                vid, name, num_frames, fps, width, height,
                is_duplicate, is_corrupt
            ) =  row
            vid = int(vid)
            num_frames = int(num_frames)
            fps = float(fps)
            width = int(width)
            height = int(height)
            is_duplicate = is_duplicate[0].upper() == 'T'
            is_corrupt = is_corrupt[0].upper() == 'T'
            channel, show, timestamp = parse_video_name(name)
            canonical_show, is_recurring = canonical_show_dict.get(show, (None, False))

            # create Channel if needed
            channel_object = get_or_create(session, schema.Channel, name=channel)

            # create CanonicalShow if needed
            if canonical_show is None:
                print('Missing canonical show for show:', show)
                canonical_show = show
            canonical_show_object = get_or_create(
                session, schema.CanonicalShow,
                name=canonical_show, is_recurring=is_recurring,
                channel_id=channel_object.id)

            # create Show if needed
            show_object = get_or_create(
                session, schema.Show, name=show,
                channel_id=channel_object.id,
                canonical_show_id=canonical_show_object.id)

            # create the Video
            session.add(schema.Video(
                id=vid, name=name, num_frames=num_frames,
            	fps=fps, width=width, height=height, time=timestamp,
                show_id=show_object.id, is_duplicate=is_duplicate,
                is_corrupt=is_corrupt))

    session.commit()


def load_hosts_staff(session, host_staff_csv):
    print('Importing channel_host, canonical_show_host')
    with open(host_staff_csv) as fp:
        fp.readline() # Skip headers
        reader = csv.reader(fp)
        for row in tqdm(reader):
            channel_name, canonical_show_name, identity_name = row

            identity_id = session.query(schema.Identity).filter_by(
                name=identity_name).one().id

            channel_id = session.query(schema.Channel).filter_by(
                name=channel_name).one().id

            if canonical_show_name == '':
                # create new host_staff_row
                session.add(
                    schema.ChannelHosts(
                        identity_id=identity_id, channel_id=channel_id))

            else:
                canonical_show_id = session.query(schema.CanonicalShow).filter_by(
                    name=canonical_show_name, channel_id=channel_id).one().id

                # create new host_staff_row
                session.add(
                    schema.CanonicalShowHosts(
                        identity_id=identity_id,
                        canonical_show_id=canonical_show_id))

    session.commit()


def load_via_copy(conn, import_path, table):
    cur = conn.cursor()
    source_csv = os.path.join(import_path, '{}.csv'.format(table))
    with open(source_csv) as fp:
        headers = fp.readline() # Skip the headers
        print('Importing {} with columns ({})'.format(table, headers[:-1]))

        # TODO disable triggers for constraints
        cur.copy_expert('copy {}({}) from stdin (format csv)'.format(table, headers), fp)
        conn.commit()


def init_worker(function, conn_args):
    function.conn = psycopg2.connect(**conn_args)


def copy_worker(args):
    source_csv, table = args
    cur = copy_worker.conn.cursor()
    with open(source_csv) as fp:
        headers = fp.readline() # Skip the headers
        cur.copy_expert('copy {}({}) from stdin (format csv)'.format(table, headers), fp)
        copy_worker.conn.commit()
    os.remove(source_csv)
copy_worker.conn = None


def split_csv(source_csv, n, out_dir):
    chunks = []
    with open(source_csv, 'r') as fp:
        headers = fp.readline()
        ofp = None
        for i, line in enumerate(fp):
            if ofp is None:
                chunk_path = os.path.join(out_dir, '{}.csv'.format(i))
                chunks.append(chunk_path)
                ofp = open(chunk_path, 'w')
                ofp.write(headers)

            ofp.write(line)

            if i % n == n - 1:
                ofp.close()
                ofp = None
        if ofp is not None:
            ofp.close()
    return chunks, headers


def parallel_load_via_copy(conn_args, import_path, table, n=100000):
    source_csv = os.path.join(import_path, '{}.csv'.format(table))
    tmp_dir = None
    try:
        tmp_dir = tempfile.mkdtemp(prefix='db-import-{}-'.format(table))
        print('Splitting {} csv: {}'.format(table, tmp_dir))
        chunks, headers = split_csv(source_csv, n, tmp_dir)
        print('Importing {} with columns ({})'.format(table, headers[:-1]))
        with Pool(initializer=init_worker, initargs=(copy_worker, conn_args)) as p:
            for _ in tqdm(
                p.imap_unordered(copy_worker, [(c, table) for c in chunks]),
                total=len(chunks)
            ):
                pass
    finally:
        if tmp_dir and os.path.isdir(tmp_dir):
            shutil.rmtree(tmp_dir)


def set_id_sequence(conn, table):
    cur = conn.cursor()
    cur.execute('SELECT MAX(id) FROM {}'.format(table))
    max_id = cur.fetchone()[0]
    print('Resetting id sequence for {} to {}'.format(table, max_id))
    cur.execute('ALTER SEQUENCE {}_id_seq RESTART WITH {}'.format(table, max_id + 1))
    conn.commit()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('import_path', type=str)
    parser.add_argument('--db-name', type=str, default='tvnews')
    parser.add_argument('--db-user', type=str, default='admin')
    return parser.parse_args()


def main(import_path, db_name, db_user):
    password = os.getenv('POSTGRES_PASSWORD')
    engine = sqlalchemy.create_engine(
        'postgresql://{}:{}@localhost/{}'.format(db_user, password, db_name))

    conn_args = {
        'dbname': db_name, 'user': db_user, 'host': 'localhost',
        'password': password
    }
    conn = psycopg2.connect(**conn_args)

    schema.Face.metadata.create_all(engine)

    Session = sqlalchemy.orm.sessionmaker(bind=engine, expire_on_commit=False)
    session = Session()

    # # Load the rest of the tables, that don't require ETL, with COPY.
    # This order matters. It must obey dependencies.
    load_via_copy(conn, import_path, 'labeler')
    load_via_copy(conn, import_path, 'frame_sampler')
    load_via_copy(conn, import_path, 'gender')
    load_via_copy(conn, import_path, 'identity')

    # videos, channel, show, and canonical_show require special pre-processing
    load_videos(session, os.path.join(import_path, 'video.csv'),
                os.path.join(import_path, 'show_to_canonical_show.csv'))

    # hosts_and_staff requires special processing as well. It depends on the
    # identity table.
    load_hosts_staff(session, os.path.join(import_path, 'hosts_and_staff.csv'))

    # These tables depend on video, and must be loaded after it
    load_via_copy(conn, import_path, 'commercial')
    parallel_load_via_copy(conn_args, import_path, 'frame')
    parallel_load_via_copy(conn_args, import_path, 'face')
    parallel_load_via_copy(conn_args, import_path, 'face_gender')
    parallel_load_via_copy(conn_args, import_path, 'face_identity')

    # Rename commercial labeler
    print('Renaming commercial labeler')
    session.query(schema.Labeler).filter_by(name='haotian-commercials').update({
        schema.Labeler.name: 'commercials'
    })
    session.commit()

    # Set sequence numbers
    conn = psycopg2.connect(dbname=db_name, user=db_user, host='localhost',
                            password=password)
    set_id_sequence(conn, 'labeler')
    set_id_sequence(conn, 'frame_sampler')
    set_id_sequence(conn, 'gender')
    set_id_sequence(conn, 'identity')
    set_id_sequence(conn, 'video')
    set_id_sequence(conn, 'commercial')
    set_id_sequence(conn, 'frame')
    set_id_sequence(conn, 'face')

    # Set up missing rows
    print('Set up new rows')
    session.add(schema.Labeler(name='handlabeled-gender', is_handlabel=True))
    session.add(schema.Labeler(name='commercials-1s', is_handlabel=False))
    session.add(schema.FrameSampler(name='1s'))

    # Drop extra mtcnn labeler
    print('Dropping extra mtcnn labeler')
    mtcnn_labeler_id = session.query(schema.Labeler).filter_by(
        name='mtcnn'
    ).one().id
    duplicate_mtcnn_labeler_id = session.query(schema.Labeler).filter_by(
        name='mtcnn:july-25-2019'
    ).one().id
    session.query(schema.Face).filter_by(
        labeler_id=duplicate_mtcnn_labeler_id).update({
            schema.Face.labeler_id: mtcnn_labeler_id})
    session.query(schema.Labeler).filter_by(
        id=duplicate_mtcnn_labeler_id
    ).delete()

    session.commit()
    print('Done!')


if __name__ == '__main__':
    main(**vars(get_args()))
