import os
from datetime import datetime
from functools import lru_cache


def parse_video_name(name: str):
    if name.endswith('.mp4'):
        name = os.path.splitext(name)[0]
    tokens = name.split('_', 3)
    if len(tokens) == 3:
        # Some videos have no show
        channel, ymd, hms = tokens
        show = '
    elif len(tokens) == 4:
        channel, ymd, hms, show = tokens
        show = show.replace('_', ' ')
    else:
        raise Exception('Incorrectly formatted show: ' + name)

    if channel in ['CNNW', 'FOXNEWSW', 'MSNBCW']:
        channel = channel[:-1]

    timestamp = datetime.strptime(ymd + hms, '%Y%m%d%H%M%S')
    return channel, show, timestamp


@lru_cache(1024 * 16)
def get_or_create(session, model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance
    else:
        instance = model(**kwargs)
        session.add(instance)
        # This flush is necessary in order to populate the primary key
        session.flush()
        return instance
