import pandas as pd
import html2text
import nltk
from nltk.tokenize import word_tokenize
from nltk.tag import pos_tag
import os
from tqdm import tqdm
import re

PATH = '/media/rohit/ext_HD/current_reports/{}/{}.html'

sec_comps = pd.read_csv('sec_comps.csv')
sec_comps = sec_comps.dropna(subset=['cik'])
sec_comps['cik'] = sec_comps['cik'].astype(int)

company_names = sec_comps['conml'].unique()

ciks = os.listdir('/media/rohit/ext_HD/current_reports')

def preprocess(text):
    text = text.replace('\n', ' ')
    return text.strip()
 
def get_names_in_other_annual_reports():
    res = []
    h = html2text.HTML2Text()
    h.ignore_links = True

    for cik in tqdm(ciks):
        path = '/media/rohit/ext_HD/current_reports/{}'.format(cik)
        years = os.listdir(path)

        for year in years:
            reports = os.listdir(os.path.join(path, year))
            for report in reports:
                report_path = '/media/rohit/ext_HD/current_reports/{}/{}/{}'.format(cik, year, report)
                if os.path.isdir(report_path):
                    continue

                with open(report_path) as f:
                    data = f.read()

                data = h.handle(data)
                data = preprocess(data)
                for name in company_names:
                    splits = name.split(' ')
                    if len(splits) == 1:
                        fuzzy_name = splits[0]
                    elif len(splits) == 2:
                        fuzzy_name = ' '.join(splits)
                    else:
                        fuzzy_name = ' '.join(splits[0:-1])
                    # print('fuzzy_name ', fuzzy_name)
                    # sentences = re.findall(r"([^.]*?'"+ fuzzy_name + "'[^.]*\.)", data) 
                    # print(sentences)
                    
                    if name in data and fuzzy_name in data:
                        # print(name, cik, year, data.count(name) + data.count(fuzzy_name))
                        res.append([name, cik, year, report, data.count(name) + data.count(fuzzy_name)])
                    elif name in data:
                        # print(name, cik, data.count(name))
                        res.append([name, cik, year, report, data.count(name)])
                    elif fuzzy_name in data:
                        # print(fuzzy_name, cik, data.count(fuzzy_name))
                        res.append([name, cik, year, report, data.count(fuzzy_name)])
                    
                    
    return res


rows = get_names_in_other_annual_reports()

res_df = pd.DataFrame(rows, columns=['company', 'appear in name', 'year', 'report_name', 'count'])
res_df.to_csv('./company_name_occurance_in_current_reports.csv', index=False)
