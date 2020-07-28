import pandas as pd
import os
import subprocess
import time
import argparse

# create necessary arguments to run the analysis
parser = argparse.ArgumentParser()
parser.add_argument('-i', '--id',
                    type=str,
                    required=True,
                    help='channel id for which video is to be scraped!!')

parser.add_argument('-y', '--year',
                    choices=[2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014,
                             2015, 2016, 2017, 2018],
                    type=int,
                    help='The years for which analysis is to be performed.')


def scrap_videos(bbc_id, year):
    path = './data/BBC/{}/{}/no_transcripts'.format(bbc_id, year)
    sources = os.listdir(path)

    for source in sources:
        source_path = os.path.join(path, source)
        source_df = pd.read_csv(source_path, index_col=0)

        for index, row in source_df.iterrows():
            source_name = row['Source']
            program = row['Program Name']
            date = row['Date']
            reason = row['Unavailable reason']

            if 'has to be requested' in reason or 'Problem' in reason:
                print('skipping {} {} for {} because {}'.format(row['Source'], row['Program Name'], row['Date'],
                                                                reason))
                continue

            output_option = '-o'
            output_name = './videos/{}-{}-{}.mp4'.format(source_name, program, date)
            video_link = row['video_link']

            if os.path.isfile(output_name):
                print('video from {} already downloaded'.format(video_link))
                continue

            print('getting video from {}'.format(video_link))
            cmd = ['youtube-dl', output_option, output_name, video_link]
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            o, e = proc.communicate()
            print('Error: ' + e.decode('ascii'))
            # delay between videos
            time.sleep(10*60)


if __name__ == '__main__':
    args = parser.parse_args()
    year = args.year
    bbc_id = args.id
    scrap_videos(bbc_id, year)
