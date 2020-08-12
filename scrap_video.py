import pandas as pd
import os
import subprocess
import time
import argparse
import request_video

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

parser.add_argument('--output_path',
                    type=str,
                    default="/media/rohit/2TB WD/videos",
                    help="the path where scraped videos are to be stored.")


def scrap_videos(bbc_id, year, output_path):
    path = './data/BBC/{}/{}/no_transcripts'.format(bbc_id, year)
    sources = os.listdir(path)

    for source in sources:
        source_path = os.path.join(path, source)
        source_df = pd.read_csv(source_path, index_col=0)

        for index, row in source_df.iterrows():
            source_name = row['Source']
            program = row['Program Name']
            date = row['Date']
            unavailable_link = row['Unavailable link']
            reason = row['Unavailable reason']

            output_option = '-o'
            output_name = '{}/videos/{}/{}/{}-{}-{}.mp4'.format(output_path, bbc_id, year, source_name, program, date)
            video_link = row['video_link']

            if os.path.isfile(output_name):
                print('video from {} already downloaded'.format(video_link))
                continue

            if 'to be requested' in reason:
                request_video.request_video(unavailable_link)
                
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
    output_path = args.output_path
    scrap_videos(bbc_id, year, output_path)
