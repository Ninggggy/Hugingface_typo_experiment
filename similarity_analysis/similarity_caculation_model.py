import pandas as pd
import itertools
from Levenshtein import distance as levenshtein_distance
from collections import defaultdict, deque

# 加载数据
file_path = 'huggingface_models'
file_path100='models_top100'
df = pd.read_excel(file_path)
df100 = pd.read_excel(file_path100)

# 将所有名称转换为字符串类型
df['Organization'] = df['Organization'].astype(str)
df['Model'] = df['Model'].astype(str)
df100['Organization'] = df100['Organization'].astype(str)
df100['Model'] = df100['Model'].astype(str)


# 查找相似的模型名，跳过相同组织的模型
def find_similar_model_names(names100, other_column100, names, other_column):
    
    similar_names = defaultdict(list)
    organization_for_models1=defaultdict(list)
    organization_for_models2=defaultdict(list)
    for (name1, other1) in zip(names100, other_column100):
        if len(name1)>9 :
            threshold=2
        # elif len(name1)>=18:
        #      threshold=3
        elif len(name1)>5 and len(name1)<=9:
                threshold=1
        else:
                threshold=0
        for (name2, other) in zip(names, other_column):
            for other2 in other:
                if other1 != other2 and levenshtein_distance(name1.lower(), name2.lower()) <= threshold: 
                    similar_names[name1].append(name2)
                    similar_names[name2].append(name1)
                    organization_for_models1[name1].append(other1)
                    organization_for_models1[other1].append(name1)
                    organization_for_models2[name2].append(other2)
                    organization_for_models2[other2].append(name2)
    return similar_names,organization_for_models1,organization_for_models2
model_names = df['Model'].unique()
model_names100 = df100['Model'].unique()
organization_for_models100 = df100.set_index('Model')['Organization'].to_dict()
organization_for_models = df.groupby('Model')['Organization'].apply(list).to_dict()
# print(f"Model to organization mapping: {organization_for_models}")
similar_models,organization_for_models1,organization_for_models2 = find_similar_model_names(model_names100, [organization_for_models100[model] for model in model_names100], model_names, [organization_for_models[model] for model in model_names])
print(f"Similar models: {similar_models}")

# 将结果转换为DataFrame以便于导出
def dict_to_df(similar_dict):
    rows = []
    for key, values in similar_dict.items():
        for value in values:
            rows.append((key, value))
    return pd.DataFrame(rows, columns=['Name1', 'Name2'])


def dict_to_df_model(similar_dict, organization_dict1, organization_dict2):
    rows = []
    a=-1
    for key, values in similar_dict.items():
        a=-1
        for value in values:
            
            a=a+1
            org1 = organization_dict1.get(key)
            org2 = organization_dict2.get(value)
            if len(values)>1 and org1 and org2 :    
                if isinstance(org2, list) and isinstance(org1,list) :
                    if a==0 :
                        for (single_org1,single_org2) in zip(org1,org2):
                            row = (key, value, single_org1, single_org2)
                            rows.append(row)
                    
                    if a>0:
                        
                        # if len(values)<=2:
                        #     for (single_org1,single_org2) in zip(org1,org2):
                        #         row = (key, value, single_org1, single_org2)
                        #         rows.append(row)
                        if values[a-1]==values[a] :
                            continue
                        
                        else:
                            for (single_org1,single_org2) in zip(org1,org2):
                                row = (key, value, single_org1, single_org2)
                                rows.append(row)
                    
                    

                        
                elif isinstance(org1,list)  :
                    if a==0   :
                        for(single_org1)in org1:
                            row = (key, value, single_org1, org2)
                            rows.append(row)
                    if a>0:
                        
                        # if len(values)<=2:
                        #     for(single_org1)in org1:
                        #         row = (key, value, single_org1, org2)
                        #         rows.append(row)
                        if values[a-1]==values[a] :
                            continue
                        
                        else:
                            for(single_org1)in org1:
                                row = (key, value, single_org1, org2)
                                rows.append(row)
                    
                            
                elif isinstance(org2,list) :
                    if a==0 :
                        for(single_org2)in org2:
                            row = (key, value, org1, single_org2)
                            rows.append(row)
                    if a>0:
                        
                        # if len(values)<=2:
                        #     for(single_org2)in org2:
                        #         row = (key, value, org1, single_org2)
                        #         rows.append(row)
                        if values[a-1]==values[a]:
                            continue
                        
                        else:
                            for(single_org2)in org2:
                                row = (key, value, org1, single_org2)
                                rows.append(row)
                    
                            
                else:
                    row = (key, value, org1, org2)
                    rows.append(row)
    
    return pd.DataFrame(rows, columns=['Name1', 'Name2', 'Author1', 'Author2'])

similar_models_df = dict_to_df_model(similar_models,organization_for_models1,organization_for_models2)

# 保存结果到Excel文件

similar_models_df.to_excel('.xlsx', index=False)
