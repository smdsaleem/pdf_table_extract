#import zipfile

import numpy as np
import pandas as pd
import Config as cfg

temp_dir = './temp/'

u_clean_empty_columns = False
u_lattice = False

def config_list(config_dict):
    global u_clean_empty_columns
    global u_lattice

    try:
        if 'tabula_clean_empty_columns' in config_dict:
            u_clean_empty_columns = config_dict['tabula_clean_empty_columns']
        elif cfg.clean_empty_columns:
            u_clean_empty_columns = cfg.clean_empty_columns
 
    except:
        u_clean_empty_columns = False
        
    try:
        if 'tabula_lattice' in config_dict:
            u_lattice = config_dict['tabula_lattice']
        elif cfg.lattice:
            u_lattice = cfg.lattice
    except:
        u_lattice = False

#def get_ordered_rows(df1, df2):
#    last_row = pd.DataFrame(df1.iloc[[-1]])
#    print("~~~~~~~~~~~~~~ Last Row:",last_row )
#    first_row = pd.DataFrame(df2.iloc[0:])
#    print("~~~~~~~~~~~~~ First Row", first_row)
#    first_row_col_one = first_row.iloc[1:]
#    res_num = re.findall('^\s*[0-9]*',first_row_col_one)
#    if not str(res_num[0]).isnumeric():
#        last_row.append(first_row)
#        print("~~~~~~~~~~~~~~ Last Row:",last_row )
#        
        
def export_json(df_page):
    
    #zipf = zipfile.ZipFile('tables.zip', 'w', zipfile.ZIP_DEFLATED)
    all_tables = {}
    counter = 0
    if df_page:
        for key in df_page:
            df_list = df_page[key]
            for i in range(len(df_list)):
                counter = counter + 1
                df = df_list[i]
                if df is not None:
                    df = df.replace(np.NAN, 'NAN', regex=True, )
                    df.reset_index()
                    #print(df)
                    #print("*" * 50)
                    d = df.to_dict(orient='split')
                    d['page_no'] = str(key + 1)
                    d = change_keynames(d)
                    all_tables['table ' + str(counter)] = d
                    #print("page=", key + 1)
    print(all_tables)
    # zipf.write('{0}{1}.json'.format(temp_dir, i + 1))
    # delete all files from temp directory
    # table_json=json.dumps(all_tables)
    # print(table_json)
    # for f in os.listdir(temp_dir):
    # os.remove(os.path.join(temp_dir, f))
    # return table_json
    return all_tables

def clean_dfs_columns(df):
    np_df_arr_col = np.array(df.columns)
    col_set = []
    for c in np_df_arr_col:        
        if not c == ' ':
            col_set.append(c)
    df2 = df[df != ' ']  
    df2.dropna(axis=1, inplace=True, how='all')
    df2.columns = col_set
    return df2

def clean_dfs_rows(df):
    df.replace(' ', np.nan, inplace=True)             
    df.dropna(axis=0, inplace=True, how='all')
#    if not df.empty:
#        df.dropna(axis=1, inplace=True, how='all')
    df = df.replace(np.nan, ' ', regex=True, )
    replacements = {'\r':' ','\x92':'\'','\x91':'\'','\x93':'\'','\x94':'\''}
    df.replace(replacements, regex=True, inplace=True)
    #df = df.replace("\r"," ", regex=True, )
    df = df.reset_index()
    del df['index']
    #print("!!!!!!!! Cleaned DF Rows:", df)
    return df

def compare_dfs(df1, df2):
    
    last_row = pd.DataFrame(df1.iloc[[-1]])
    first_row = pd.DataFrame(df2.iloc[0:])
    df = pd.DataFrame()
  
    if first_row.shape[1] == last_row.shape[1]:
        if not first_row.columns.equals(last_row.columns):
            np_arr_col = np.array(first_row.columns)
            np2= np.vstack((np_arr_col, np.array(first_row)))
            df2_mod = pd.DataFrame(np2, columns = last_row.columns)
        else:
            df2_mod = df2

        #get_ordered_rows(df1,df2_mod)
        df = pd.concat([df1,df2_mod])
        df = df.reset_index()
        del df['index']

    return df

def export_combined_json(df_page, config_dict):
    config_list(config_dict)
    #zipf = zipfile.ZipFile('tables.zip', 'w', zipfile.ZIP_DEFLATED)
    all_tables = {}
    counter = 0
    npage = 0
    if df_page:
        last_df = pd.DataFrame()
        for key in df_page:
            page_no = ""
            npage = npage + 1
            df_list = df_page[key]
            first_df = pd.DataFrame()
            #is_last = False
            for i in range(len(df_list)):
                is_last_table_deleted = False
                counter = counter + 1
                df = df_list[i]
                if df is not None:
                   
                    if i == 0 and npage > 1:
                        #print ("Assigning First DF:", df)
                        first_df = df
                         
                    if npage > 1 and not first_df.empty and not last_df.empty:                        
                        res_df = compare_dfs(last_df, first_df)
                        #print ("Result DF: ", res_df)
                        if not res_df.empty:
                            df = res_df
                            counter = counter - 1
                            temp_dict = all_tables['table ' + str(counter)]
                            page_no = temp_dict['page_no']
                            del all_tables['table ' + str(counter)]                            
                            is_last_table_deleted = True
                            last_df = pd.DataFrame()
                            first_df = pd.DataFrame()
                         
                    if i == (len(df_list) -1):
                        #print ("Assigning Last DF:", df)
                        last_df = df
                    
                    #df = df.replace(np.NAN, 'NAN', regex=True, )
                    df.reset_index()
                    #print(df)
                    #print("*" * 50)
                    df =  clean_dfs_rows(df)
                    if u_clean_empty_columns and u_lattice:                        
                        df = clean_dfs_columns(df)
                        #df = get_ordered_rows(df)
                    d = df.to_dict(orient='split')
                    if not is_last_table_deleted:
                        d['page_no'] = str(key + 1)
                    else:
                        d['page_no'] = page_no + ", " + str(key+1)
                        
                    d = change_keynames(d)
                    all_tables['table ' + str(counter)] = d

                    #print("page=", key + 1)
    print(all_tables)
    # zipf.write('{0}{1}.json'.format(temp_dir, i + 1))
    # delete all files from temp directory
    # table_json=json.dumps(all_tables)
    # print(table_json)
    # for f in os.listdir(temp_dir):
    # os.remove(os.path.join(temp_dir, f))
    # return table_json
    return all_tables

def change_keynames(d):
    # val=d['index']
    # d['no of rows']=val
    del d['index']

    val = d['columns']
    d['column_names'] = val
    del d['columns']

    val = d['data']
    d['column_values'] = val
    del d['data']
    return d

