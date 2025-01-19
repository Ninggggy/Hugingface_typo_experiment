import sys
import ast
import itertools
from difflib import SequenceMatcher
from collections import defaultdict
import gc
import math
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from Levenshtein import distance as levenshtein_distance
# Read data
file_path = 'similar_names_and_info_final.csv'
file_path2='huggingface_organizations_scraped_link.xlsx'
def levenshtein_similarity(name1, name2):
    # Calculate the Levenshtein distance between the two strings
    levenshtein_dist = levenshtein_distance(name1, name2)
    
    # Calculate the maximum length of the two strings
    name_length = max(len(name1), len(name2))
    
    # Normalize the Levenshtein distance to a similarity score
    similarity = 1 - (levenshtein_dist / name_length)
    
    return similarity



def get_common_words():
    # 定义常见通用词汇列表
    return [
        'research', 'project', 'ai', 'ml', 'deep', 'learning', 'official', 
        'studio', 'lab', 'tech', 'technology', 'technologies', 'dev', 
        'development', 'data', 'science', 'model', 'neural', 'net', 'network',
        'intelligence', 'artificial', 'machine', 'hub', 'platform', 'group',
        'team', 'org', 'organization', 'foundation', 'institute', 'inc','solution','university','organisation'
    ]


def remove_common_words(name):
    # 移除通用词汇
    common_words = get_common_words()
    name_parts = name.split()
    return ' '.join([word for word in name_parts if word.lower() not in common_words])

df = pd.read_csv(file_path)
df2=pd.read_excel(file_path2)
def compete_unmatchscore(df):
    match_scores = [0] * len(df)
    curl_nums=[0]*len(df)
    title_nums=[0]*len(df)
    web_nums=[0]*len(df)
    social_nums=[0]*len(df)
    add_nums=[0]*len(df)
    for idx1, row1 in df.iterrows():
        name1 = str(row1['name']).lower()
        name2 = str('/'+row1['Similar Name']).replace("无法判断", "")
        print(name2)
        # filtered_df = df2[df2['Organization Name'] == name2]
        filtered_df = df2.loc[df2['Organization Name'].str.lower() == name2.lower()]
        row2 = filtered_df.iloc[0]
        name2=str(row1['Similar Name']).replace("无法判断", "").lower()
        social1=str(row1['social_media']).replace("huggingface.co/", "").replace("twitter.com/", "").replace("github.com/", "").replace('http','').replace('/','').lower()
        social2=str(row2['social_media']).replace("huggingface.co/", "").replace("twitter.com/", "").replace("github.com/", "").replace('http','').replace('/','').lower()
        weblink1=str(row1['website_link']).replace("huggingface.co/", "").replace("twitter.com/", "").replace("github.com/", "").replace('http','').replace('/','').lower()
        weblink2=str(row2['website_link']).replace("huggingface.co/", "").replace("twitter.com/", "").replace("github.com/", "").replace('http','').replace('/','').lower()

        match_score=0
        # Comparison conditions
        social_match = (name1 in social2
                        or name2 in social1)
        social_matchand = (name1 in social2
                        and name2 in social1)            

                # 计算Levenshtein距离
        # name_length = max(len(name1), len(name2))
        curl_levenshtein_sim = levenshtein_similarity(name1,name2)
        print(f"Comparing: {row1['title']} with {row2['title']}")
        title_match = SequenceMatcher(None,str(row1['title']).lower(), 
                            str(row2['title']).lower()).ratio() 
        
        weblink_match=(name1 in weblink2
                        or name2 in weblink1)
        weblink_matchand=(name1 in weblink2
                        and name2 in weblink1)            

        if weblink_matchand:
            if levenshtein_similarity(name1,name2)>0.7:
                web_num=20
            else:
                web_num=0
        elif weblink_match:
            if levenshtein_similarity(name1,name2)>0.7 or levenshtein_similarity(name1,name2)>0.7:
                web_num=10
            else:
                web_num=0
        else:
            web_num=0

        curl_num=curl_levenshtein_sim*35
        
        title_num=title_match*35

        if social_matchand:
            social_num=10
        elif social_match:
            social_num=5
        else:
            social_num=0
        
        if social_num==0 and web_num==0:
            if weblink1 and weblink2:
                add_num=SequenceMatcher(None, name1,name2).ratio()*5
            elif social1 and social2:
                add_num=SequenceMatcher(None, social1,social2).ratio()*5
            else:
                add_num=0
        else:
            add_num=0
        match_score=social_num+web_num+curl_num+title_num+add_num

                # 惩罚因子：如果名称中包含过多通用词汇，降低得分
        original_names = f"{name1} {name2}".lower()
        print(original_names)
        common_words=get_common_words()
        common_words_count1 = sum(1 for word in common_words
                               if word in name1)
        common_words_count2=sum(1 for word in common_words
                               if word in name2)
        common_words_count=common_words_count2+common_words_count1
        print(common_words_count)
        if common_words_count > 1 and match_score <77.5:
            penalty = 0.8 ** common_words_count  # 指数衰减惩罚
            match_score *= penalty

        # unmatch_scores[idx1] = unmatch_score
        match_scores[idx1] = match_score
        curl_nums[idx1] =curl_num
        title_nums[idx1] =title_num
        web_nums[idx1] =web_num
        social_nums[idx1] =social_num
        add_nums[idx1]=add_num
        # df.loc[:, 'unmatch_score'] = pd.Series(unmatch_scores, index=df.index)
        df.loc[:, 'match_score'] = pd.Series(match_scores, index=df.index)
        df.loc[:, 'curl_num'] = pd.Series(curl_nums, index=df.index)
        df.loc[:, 'title_num'] = pd.Series(title_nums, index=df.index)
        df.loc[:, 'web_num'] = pd.Series(web_nums, index=df.index)
        df.loc[:, 'social_num'] = pd.Series(social_nums, index=df.index)
        df.loc[:, 'add_num'] = pd.Series(add_nums, index=df.index)
    return df

# Convert specified columns to string type
columns_to_convert = ['name', 'canonical_url', 'title', 'website_link','social_media','members_title','collections_title','spaces_href','models_href','datasets_href','numbers','downloads','stars','Similar Name']
for col in columns_to_convert:
    df[col] = df[col].astype(str)

# Use the modified function
df= compete_unmatchscore(df)
# 按总分降序排序
result_df = df.sort_values(by='match_score', ascending=False)
# Output the modified dataframe with 'unmatch_score' column
output_excel_filename = 'analys_similar_names_and_info.csv'
result_df.to_csv(output_excel_filename, index=False)

print(f'Modified dataframe with unmatch scores has been saved to {output_excel_filename}')