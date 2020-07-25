import pandas as pd
import os
import subprocess
import time


def scrap_videos():
    path = './data/BBC/54/2016/no_transcripts'
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
            output_template = './videos/{}-{}-{}'.format(source_name, program, date)
            video_link = row['video_link']
            print('getting video from {}'.format(video_link))
            cmd = ['youtube-dl', output_option, output_template, video_link]
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            o, e = proc.communicate()
            print('Error: ' + e.decode('ascii'))
            # delay between videos
            time.sleep(10*60)


if __name__ == '__main__':
    scrap_videos()
