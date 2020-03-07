import os
import csv
from datetime import datetime
from pytz import timezone
import schema
import sqlalchemy
import psycopg2
import tqdm


def parse_video_name(name: str):
    if name.endswith('.mp4'):
        name = os.path.splitext(name)[0]
    tokens = name.split('_', 3)
    if len(tokens) == 3:
        # Some videos have no show
        channel, ymd, hms = tokens
        show = ""
    elif len(tokens) == 4:
        channel, ymd, hms, show = tokens
        show = show.replace("_", " ")
    else:
        raise Exception("Incorrectly formatted show: " + name)

    if channel in ['CNNW', 'FOXNEWSW', 'MSNBCW']:
        channel = channel[:-1]
    timestamp = datetime.strptime(ymd + hms, '%Y%m%d%H%M%S')
    return channel, show, timestamp

def get_or_create(model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance
    else:
        instance = model(**kwargs)
        session.add(instance)
        return instance

def load_videos(video_csv, show_to_canonical_show_csv):
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
        for row in tqdm.tqdm(reader):
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
            channel_object = get_or_create(schema.Channel, name=channel)

            # create CanonicalShow if needed
            if canonical_show is None:
                print('Missing canonical show for show:', show)
                canonical_show = show
            canonical_show_object = get_or_create(schema.CanonicalShow,
                name=canonical_show, is_recurring=is_recurring,
                channel_id=channel_object.id)

            # create Show if needed
            show_object = get_or_create(schema.Show,
                name=show,
                channel_id=channel_object.id,
                canonical_show_id=canonical_show_object.id)

            # create the Video
            session.add(schema.Video(name=name, num_frames=num_frames, fps=fps,
                width=width, height=height, time=timestamp,
                show_id=show_object.id, is_duplicate=is_duplicate, is_corrupt=is_corrupt))

    session.commit()

def load_hosts_staff(host_staff_csv):
    conn = engine.connect()
    with open(host_staff_csv) as fp:
        fp.readline() # Skip headers
        reader = csv.reader(fp)
        for row in tqdm.tqdm(reader):
            channel_name, canonical_show_name, identity_name = row

            s = sqlalchemy.select([schema.Identity]).where(
                schema.Identity.name == identity_name)
            identity_id = conn.execute(s).fetchone()['id']
            
            if channel_name != '':
                s = sqlalchemy.select([schema.Channel]).where(
                    schema.Channel.name == channel_name)
                channel_id = conn.execute(s).fetchone()['id']

                # create new host_staff_row
                session.add(schema.HostsAndStaff(identity_id=identity_id,
                    channel_id=channel_id))
            
            if canonical_show_name != '':
                s = sqlalchemy.select([schema.CanonicalShow]).where(
                    schema.CanonialShow.name == canonical_show_name)
                canonical_show_id = conn.execute(s).fetchone()['id']
                
                # create new host_staff_row
                session.add(schema.HostsAndStaff(identity_id=identity_id,
                    canonical_show_id=canonical_show_id))

    session.commit()

def load_via_copy(import_path, table):
    source_csv = "{}{}.csv".format(import_path, table)
    with open(source_csv) as fp:
        headers = fp.readline() # Skip the headers
        print("Importing {} with columns ({})".format(table, headers[:-1]))

        cur.copy_expert("copy {}({}) from stdin (format csv)".format(table, headers), fp)
        conn.commit()

if __name__ == '__main__':
    password = os.getenv("POSTGRES_PASSWORD")
    engine = sqlalchemy.create_engine("postgresql://admin:{}@localhost/tvnews".format(password))

    conn = psycopg2.connect(dbname="tvnews", user="admin", host='localhost', password=password)
    cur = conn.cursor()

    schema.Face.metadata.drop_all(engine)
    schema.Face.metadata.create_all(engine)

    Session = sqlalchemy.orm.sessionmaker(bind=engine)
    session = Session()

    import_path = '/newdisk/export_db/'

    # Load the rest of the tables, that don't require ETL, with COPY.
    # This order matters. It must obey dependencies.
    load_via_copy(import_path, "labeler")
    load_via_copy(import_path, "frame_sampler")
    load_via_copy(import_path, "gender")
    load_via_copy(import_path, "identity")

    # videos, channel, show, and canonical_show require special pre-processing
    load_videos(import_path + 'video.csv', import_path + 'show_to_canonical_show.csv')

    # hosts_and_staff requires special processing as well. It depends on the
    # identity table.
    load_hosts_staff(import_path + 'hosts_and_staff.csv')

    # These tables depend on video, and must be loaded after it
    load_via_copy(import_path, "frame")
    load_via_copy(import_path, "face")
    load_via_copy(import_path, "face_gender")
    load_via_copy(import_path, "face_identity")
    load_via_copy(import_path, "commercial")
    
