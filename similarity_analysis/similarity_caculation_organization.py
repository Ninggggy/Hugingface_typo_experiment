import sys
sys.path.append('./.local/lib/python3.10/site-packages')
import ast
import itertools
from difflib import SequenceMatcher
from collections import defaultdict
import gc
import math
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from Levenshtein import distance as levenshtein_distance
def similar_sequen(a, b):
    return SequenceMatcher(None, a, b).ratio()
    
def similar(s1, s2):
    return levenshtein_distance(s1, s2)

def find_similar_names_and_info(df, batch_size=1000):
    similar_info = defaultdict(list)
    total_rows = len(df)
    total_batches = math.ceil(total_rows / batch_size)

    # Preprocess data
    # dfl['Organization Name'] = df['Organization Name'].str.lower().str.replace("/", "")
    # dfl['social_media'] = df['social_media'].str.lower()
    # dfl['canonical_url'] = df['canonical_url'].str.lower()
    # dfl['website_link'] = df['website_link'].str.lower()
    # dfl['title'] = df['title'].str.lower()

    # Process data in batches using ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_batch, df, batch_number, batch_size, total_rows, similar_info)
                   for batch_number in range(total_batches)]
        for future in futures:
            future.result()  # Wait for all batches to complete
    
    return similar_info

def process_batch(df, batch_number, batch_size, total_rows, similar_info):
    start_index = batch_number * batch_size
    end_index = min(start_index + batch_size, total_rows)
    batch_df = df.iloc[start_index:end_index]
    total_batches = math.ceil(total_rows / batch_size)
    
    for idx1, row1 in batch_df.iterrows():
        # Compare only with subsequent rows to avoid duplicate comparisons
        for idx2, row2 in df.iloc[idx1 + 1:].iterrows():
            name1 = str(row1['Organization Name']).lower().replace("/", "")
            name2 = str(row2['Organization Name']).lower().replace("/", "")
            social1=str(row1['social_media']).replace("huggingface", "").replace("twitter", "").replace("github", "").lower()
            social2=str(row2['social_media']).replace("huggingface", "").replace("twitter", "").replace("github", "").lower()
            weblink1=str(row1['website_link']).replace("huggingface", "").replace("twitter", "").replace("github", "").lower()
            weblink2=str(row2['website_link']).replace("huggingface", "").replace("twitter", "").replace("github", "").lower()
# Define threshold
            threshold_match=0.7
            if len(name1) > 8:
                threshold_url = 2
            elif 4 < len(name1) <= 8:
                threshold_url = 1
            else:
                threshold_url = 0
        
            # Comparison conditions
            social_match = (name1 in social2
                          or name2 in social1)
            
            curl_direct_match = similar(name1, 
                                    name2) <= threshold_url
            
            title_match = similar_sequen(str(row1['title']).lower(), 
                                str(row2['title']).lower()) >= threshold_match
            
            weblink_match=(name1 in weblink2
                          or name2 in weblink1)
            
            if weblink_match or social_match or curl_direct_match or title_match:
                if sum(ast.literal_eval(row1['downloads']))>sum(ast.literal_eval(row2['downloads'])):
                    info2 = {
                        'name': row2['Organization Name'],
                        'canonical_url': row2['canonical_url'],
                        'website_link':row2['website_link'],
                        'title': row2['title'],
                        'social_media': row2['social_media'],
                        'members_title':row2['team_members_title'],
                        'collections_title':row2['collections_title'],
                        'spaces_href':row2['spaces_href'],
                        'models_href':row2['models_href'],
                        'datasets_href':row2['datasets_href'],
                        'numbers':row2['numbers'],
                        'downloads':row2['downloads'],
                        'stars':row2['stars']
                    }
                    
                    similar_info[name1].append(info2)

                elif sum(ast.literal_eval(row1['downloads']))<sum(ast.literal_eval(row2['downloads'])):
                    info1 = {
                        'name': row1['Organization Name'],
                        'canonical_url': row1['canonical_url'],
                        'website_link':row1['website_link'],
                        'title': row1['title'],
                        'social_media': row1['social_media'],
                        'members_title':row1['team_members_title'],
                        'collections_title':row1['collections_title'],
                        'spaces_href':row1['spaces_href'],
                        'models_href':row1['models_href'],
                        'datasets_href':row1['datasets_href'],
                        'numbers':row1['numbers'],
                        'downloads':row1['downloads'],
                        'stars':row1['stars']
                    }
                    similar_info[name2].append(info1)

                else:
                    if sum(ast.literal_eval(row1['stars']))>sum(ast.literal_eval(row2['stars'])):
                        info2 = {
                            'name': row2['Organization Name'],
                            'canonical_url': row2['canonical_url'],
                            'website_link':row2['website_link'],
                            'title': row2['title'],
                            'social_media': row2['social_media'],
                            'members_title':row2['team_members_title'],
                            'collections_title':row2['collections_title'],
                            'spaces_href':row2['spaces_href'],
                            'models_href':row2['models_href'],
                            'datasets_href':row2['datasets_href'],
                            'numbers':row2['numbers'],
                            'downloads':row2['downloads'],
                            'stars':row2['stars']
                        }
                        similar_info[name1].append(info2)
                    elif sum(ast.literal_eval(row1['stars']))<sum(ast.literal_eval(row2['stars'])):
                        info1 = {
                            'name': row1['Organization Name'],
                            'canonical_url': row1['canonical_url'],
                            'website_link':row1['website_link'],
                            'title': row1['title'],
                            'social_media': row1['social_media'],
                            'members_title':row1['team_members_title'],
                            'collections_title':row1['collections_title'],
                            'spaces_href':row1['spaces_href'],
                            'models_href':row1['models_href'],
                            'datasets_href':row1['datasets_href'],
                            'numbers':row1['numbers'],
                            'downloads':row1['downloads'],
                            'stars':row1['stars']
                        }
                        similar_info[name2].append(info1)                  
                    else:
                        if sum(ast.literal_eval(row1['numbers']))>sum(ast.literal_eval(row2['numbers'])):
                            info2 = {
                                'name': row2['Organization Name'],
                                'canonical_url': row2['canonical_url'],
                                'website_link':row2['website_link'],
                                'title': row2['title'],
                                'social_media': row2['social_media'],
                                'members_title':row2['team_members_title'],
                                'collections_title':row2['collections_title'],
                                'spaces_href':row2['spaces_href'],
                                'models_href':row2['models_href'],
                                'datasets_href':row2['datasets_href'],
                                'numbers':row2['numbers'],
                                'downloads':row2['downloads'],
                                'stars':row2['stars']
                            }
                            similar_info[name1].append(info2)
                        elif sum(ast.literal_eval(row1['numbers']))<sum(ast.literal_eval(row2['numbers'])):
                            info1 = {
                                'name': row1['Organization Name'],
                                'canonical_url': row1['canonical_url'],
                                'website_link':row1['website_link'],
                                'title': row1['title'],
                                'social_media': row1['social_media'],
                                'members_title':row1['team_members_title'],
                                'collections_title':row1['collections_title'],
                                'spaces_href':row1['spaces_href'],
                                'models_href':row1['models_href'],
                                'datasets_href':row1['datasets_href'],
                                'numbers':row1['numbers'],
                                'downloads':row1['downloads'],
                                'stars':row1['stars']
                            }
                            similar_info[name2].append(info1)     

                        else:
                            info1 = {
                                'name': row1['Organization Name'],
                                'canonical_url': row1['canonical_url'],
                                'website_link':row1['website_link'],
                                'title': row1['title'],
                                'social_media': row1['social_media'],
                                'members_title':row1['team_members_title'],
                                'collections_title':row1['collections_title'],
                                'spaces_href':row1['spaces_href'],
                                'models_href':row1['models_href'],
                                'datasets_href':row1['datasets_href'],
                                'numbers':row1['numbers'],
                                'downloads':row1['downloads'],
                                'stars':row1['stars']
                            }                            
                            similar_info['无法判断'+str(row2['Organization Name']).replace("/", "")].append(info1) 
    
    # Regularly perform garbage collection
    gc.collect()
    
    # Print progress
    print(f"Processed batch {batch_number + 1} of {total_batches}: {end_index} out of {total_rows} rows",flush=True)



# Rest of the code remains the same...
# Read data
file_path = 'huggingface_organizations_scraped_link.xlsx'
df = pd.read_excel(file_path)

# Convert specified columns to string type
columns_to_convert = ['Organization Name', 'canonical_url', 'title', 'website_link','social_media','team_members_title','collections_title','spaces_href','models_href','datasets_href','numbers','downloads','stars']
for col in columns_to_convert:
    df[col] = df[col].astype(str)

# Use the modified function
similar_info = find_similar_names_and_info(df, batch_size=1000)

# Create results DataFrame
results = []
for name, info_list in similar_info.items():
    for info in info_list:
        info['Similar Name'] = name
        results.append(info)

df_similar_info = pd.DataFrame(results)

# Save results
excel_filename = 'similar_names_and_info_final.csv'
df_similar_info.to_csv(excel_filename, index=False)

print(f'Data has been saved to {excel_filename}')
