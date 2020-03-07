import os
import csv
from datetime import datetime
from pytz import timezone
​
​
UTC = timezone('UTC')
ET = timezone('US/Eastern')
​
​
def parse_video_name(name: str,  tz: timezone):
    if name.endswith('.mp4'):
        name = os.path.splitext(name)[0]
    channel, ymd, hms, show = name.aplit('_', 3)
    if channel in ['CNNW', 'FOXNEWSW', 'MSNBCW']:
        channel = channel[:-1]
    timestamp = datetime.strptime(ymd + hms, '%Y%m%d%H%M%S')
    timestamp_tz = timestamp.replace(tzinfo=UTC).astimezone(tz=tz)
    return channel, show, timestamp_tz
​
​
def load_videos(video_csv, canonical_show_csv):
    canonical_show_dict = {}
    with open(canonical_show_csv) as fp:
        reader = csv.reader(fp)
        for row in reader:
            show, canonical_show, is_recurring = row
            canonical_show_dict[show] = (canonical_show, is_recurring)
​
    with open(video_csv) as fp:
        reader = csv.reader(fp)
        for row in reader:
            (
                vid, name, num_frames, fps, width, height,
                is_duplicate, is_corrupt
            ) =  row
            vid = int(vid)
            num_frames = int(num_frames)
            width = int(width)
            height = int(height)
            is_duplicate = is_duplicate[0].upper() == 'T'
            is_corrupt = is_corrupt[0].upper() == 'T'
            channel, show, timestamp = parse_video_name(name)
            canonical_show, is_recurring = canonical_show.get(show, (None, False))
​
            # TODO: create channel if needed
​
            # TODO: create (channel, show) if needed
​
            # TODO: create (channel, canonical_show) if needed
            if canonical_show is None:
                print('Missing canonical show for show:', show)
                canonical_show = show
​
            # TODO: create video
​
​
def load_hosts_staff(host_staff_csv):
    with open(host_staff_csv) as fp:
        reader = csv.reader(fp)
        for row in reader:
            channel_name, canonical_show_name, identity_name = row
            identity_id = 0 # TODO: lookup
            if channel_name != '':
                channel_id = 0 # TODO: lookup
                # TODO: create new host_staff_row
            if canonical_show_name != '':
                canonical_show_id = 0 # TODO: lookup
                # TODO: create new host_staff_row

if __name__ == '__main__':
    print("hello")

    password = os.getenv("POSTGRES_PASSWORD")
    engine = create_engine("postgresql://admin:{}@localhost/postgres".format(password))

    conn = psycopg2.connect(dbname="tvnews", user="admin", host='localhost', password=password)
    cur = conn.cursor()

    schema.Face.metadata.create_all(engine)

    # TODO Populate the tables with data. They are sorted in dependency order.

    cur.close()
    conn.close()
