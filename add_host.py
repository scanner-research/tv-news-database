#!/usr/bin/env python3

import argparse
import os

import schema
from util import get_db_session


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('name', type=str)
    parser.add_argument('-c', '--channel', type=str,
                        choices=['CNN', 'FOXNEWS', 'MSNBC'])
    parser.add_argument('-s', '--canonical_show', type=str)
    parser.add_argument('--db-name', type=str, default='tvnews')
    parser.add_argument('--db-user', type=str, default='admin')
    return parser.parse_args()


def main(name, channel, canonical_show, db_name, db_user):
    password = os.getenv('POSTGRES_PASSWORD')
    session = get_db_session(db_user, password, db_name)

    name = name.lower()

    person_obj = session.query(schema.Identity).filter_by(name=name).one()
    print('{} is a host of the following:'.format(person_obj.name))
    for x in session.query(schema.ChannelHosts, schema.Channel).filter(
        schema.ChannelHosts.channel_id == schema.Channel.id
    ).filter_by(identity_id=person_obj.id).all():
        print('\t{}'.format(x[1].name))

    if input('Proceed (y/N): ').lower() != 'y':
        raise Exception('User aborted!')

    channel_obj = None
    if channel is not None:
        print('Making {} a host of {}.'.format(name, channel))
        channel_obj = session.query(schema.Channel).filter_by(
            name=channel).one()

    c_show_obj = None
    if canonical_show is not None:
        print('Making {} a host of {}.'.format(name, canonical_show))
        c_show_obj = session.query(schema.CanonicalShow).filter_by(
            name=canonical_show).one()

    if channel_obj is not None:
        channel_host_obj = schema.ChannelHosts(
            channel_id=channel_obj.id, identity_id=person_obj.id)
        session.add(channel_host_obj)

    if c_show_obj is not None:
        c_show_host_obj = schema.CanonicalShowHosts(
            canonical_show_id=c_show_obj.id,
            identity_id=person_obj.id)
        session.add(c_show_host_obj)

    session.flush()
    session.commit()
    print('Done!')


if __name__ == '__main__':
    main(**vars(get_args()))
