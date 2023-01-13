#!/usr/bin/env python3

import os
import argparse
import shutil
from multiprocessing import Pool
from subprocess import check_call
from tqdm import tqdm


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('pipeline_output_dir')
    parser.add_argument('out_dir')
    return parser.parse_args()


def compress_worker(args):
    video, out_dir = args

    tmp_file = '{}.tar.gz'.format(video)
    cmd = ' '.join(['tar', '-czf', tmp_file, video])
    print('Run:', cmd)
    check_call(cmd, shell=True)

    out_file = os.path.join(out_dir, tmp_file)
    print('Move:', tmp_file, '->', out_file)
    shutil.move(tmp_file, out_file)


def main(pipeline_output_dir, out_dir):
    out_dir = os.path.abspath(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    os.chdir(pipeline_output_dir)
    worker_args = []
    for video in os.listdir():
        if os.path.isdir(video):
            worker_args.append((video, out_dir))

    with Pool() as p:
        for _ in tqdm(
            p.imap_unordered(compress_worker, worker_args),
            total=len(worker_args)
        ):
            pass
    print('Done!')


if __name__ == '__main__':
    main(**vars(get_args()))
