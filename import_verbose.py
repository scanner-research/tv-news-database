import os
import csv
from datetime import datetime
from pytz import timezone
import schema
import sqlalchemy


def parse_video_name(name: str):
    if name.endswith('.mp4'):
        name = os.path.splitext(name)[0]
    channel, ymd, hms, show = name.aplit('_', 3)
    if channel in ['CNNW', 'FOXNEWSW', 'MSNBCW']:
        channel = channel[:-1]
    timestamp = datetime.strptime(ymd + hms, '%Y%m%d%H%M%S')
    return channel, show, timestamp_

def get_or_create(model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance
    else:
        instance = model(**kwargs)
        session.add(instance)
        return instance

def load_videos(video_csv, canonical_show_csv):
    canonical_show_dict = {}
    with open(canonical_show_csv) as fp:
        reader = csv.reader(fp)
        for row in reader:
            show, canonical_show, is_recurring = row
            canonical_show_dict[show] = (canonical_show, is_recurring)

    with open(video_csv) as fp:
        reader = csv.reader(fp)
        for row in reader:
            print(row)
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
            canonical_show, is_recurring = canonical_show.get(show, (None, False))

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
                channel_id=channel_object.id,
                canonical_show_id=canonical_show_object.id, show=show)

            # create the video
            session.add(schema.Video(name=name, num_frames=num_frames, fps=fps,
                width=width, height=height, time=timestamp,
                show_id=show_object.id, is_duplicate=is_duplicate, is_corrupt=is_corrupt))

    session.commit()

def load_hosts_staff(host_staff_csv):
    with open(host_staff_csv) as fp:
        reader = csv.reader(fp)
        for row in reader:
            channel_name, canonical_show_name, identity_name = row

            s = sqlalchemy.select([schema.Identity]).where(name=identity_name)
            identity_id = conn.execute(s).fetchone().id
            
            if channel_name != '':
                s = sqlalchemy.select([schema.Channel]).where(name=channel_name)
                channel_id = conn.execute(s).fetchone().id

                # create new host_staff_row
                session.add(schema.HostsAndStaff(identity_id=identity_id,
                    channel_id=channel_id))
            
            if canonical_show_name != '':
                s = sqlalchemy.select([schema.CanonicalShow]).where(name=canonical_show_name)
                canonical_show_id = conn.execute(s).fetchone().id
                
                # create new host_staff_row
                session.add(schema.HostsAndStaff(identity_id=identity_id,
                    canonical_show_id=canonical_show_id))

    session.commit()

def load_via_copy(import_path):
    for table in ["commercial", "frame_sampler", "gender", "labeler"]:
            # , "face", "face_gender", "face_identity", "frame", "identity"]:
        source_csv = "{}{}.csv".format(import_path, table)
        print(table, source_csv)

        # fd = open("/newdisk/trimmed/{}.csv".format(table_name))
        # headers = fd.readline() # Skip the headers
        # print(headers)

        # cur.copy_expert("copy {}({}) from stdin (format csv)".format(table_name, headers), fd)
        # conn.commit()

if __name__ == '__main__':
    password = os.getenv("POSTGRES_PASSWORD")
    engine = sqlalchemy.create_engine("postgresql://admin:{}@localhost/tvnews".format(password))

    conn = psycopg2.connect(dbname="tvnews", user="admin", host='localhost', password=password)
    cur = conn.cursor()

    schema.Face.metadata.create_all(engine)

    Session = sqlalchemy.orm.sessionmaker(bind=engine)
    session = Session()

    # import_path = '/newdisk/export_db/'

    # Populate the tables with data
    # load_hosts_staff(import_path + 'host_and_staff.csv')

    # load_videos(import_path + 'video.csv', import_path + 'canonical_show.csv')

    # Load the rest of the tables, that don't require ETL, with COPY
    load_via_copy(import_path)
