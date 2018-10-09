from PyPDF2 import PdfFileReader
from tabula import read_pdf

import Utils
import Config as cfg

pdf_encoding = 'latin1'
temp_dir = './temp/'

u_lattice = False
u_stream = True
u_guess = True
#u_encoding = 'ISO-8859-1'
#u_encoding = 'utf-8'
u_encoding = 'latin1'
config_dict = {}

def config_list():
    global u_lattice
    global u_stream
    global u_guess
    global u_encoding
    
    try:
        if 'tabula_lattice' in config_dict:
            u_lattice = config_dict['tabula_lattice']
        elif cfg.lattice:
            u_lattice = cfg.lattice
    except:
        u_lattice = False
            
    try:
        if 'tabula_stream' in config_dict:
            u_stream = config_dict['tabula_stream']
        elif cfg.stream:
            u_stream = cfg.stream
    except:
        u_stream = True
            
    try:
        if 'tabula_guess' in config_dict:
            u_guess = config_dict['tabula_guess']
        elif cfg.guess:
            u_guess = cfg.guess
    except:
        u_guess = True   
            
    try:
        if 'tabula_encoding' in config_dict:
            u_encoding = config_dict['tabula_encoding']
        elif cfg.encoding:
            u_encoding = cfg.encoding
    except:
        u_encoding = 'latin1'


# df=read_pdf('D:\\projects\\macquire\\3.pdf',pages='18',encoding='latin1')
# #print(df.shape)
# print(df)
# df.to_csv('test.csv')

# for tb in df:
#   print('=========',tb.shape)

def extractAllTables(pdfPath):
    df_list = read_pdf(pdfPath, pages='all', multiple_tables=True, encoding=pdf_encoding, lattice=False, stream=False)
    
    return df_list


def extractTablesByPage(pdfPath, pageNo):
    try:
        config_list()
        #print("Analyzing Page ", pageNo)
        df = read_pdf(pdfPath, pages=str(pageNo), multiple_tables=True,encoding=u_encoding,lattice=u_lattice,stream=u_stream,guess =u_guess)
        #df = read_pdf(pdfPath, pages=str(pageNo), multiple_tables=True, encoding=pdf_encoding, lattice=True, stream=False, guess = True)

        print("End of Analyzing Page ", pageNo, "DF:", df)
    except RuntimeError:
        print("parse error")
    return df


def get_no_pages(filepath):
    with open(filepath, "rb") as f:
        pdf = PdfFileReader(f, 'rb')
        noofpages = pdf.getNumPages()
    return noofpages


def getTablesFromPdf(pdfPath, l_config_dict):
    global config_dict
    config_dict = l_config_dict
    # df_list=extractAllTables(pdfPath)
    noofpages = get_no_pages(pdfPath)

    page_df_processed = {}
    # if df_list:
    for i in range(noofpages):
        try:
            # df = df_list[i]
            df_list_processed = []
            df_list = extractTablesByPage(pdfPath, i + 1)
            for df in df_list:
                if df is not None and not df.empty:
                    # print("new df",df)
                    df = df.fillna(" ")
                    #print ("Check for NAs: ", df)
                    df.columns = df.iloc[0]
                    # Saleem: is this df or df.columns?
                    #heads = [x for x in list(df) if str(x) != 'nan']
                    heads = [x for x in list(df.columns) ]
                    #heads_df = pd.DataFrame(np.array(heads))
                    #heads_df.fillna("-")
                    df = df.iloc[1:]
                    # df.dropna(axis=1, how='all',inplace=True)
#                    print("=" * 40)
#                    print("head columns=", heads)
#                    print(df)

                    if not df.empty:
                    #if not df.empty and (len(df.columns) == len(heads)):
                        df.columns = heads
                        df.reset_index()
                        df_list_processed.append(df)
                        page_df_processed[i] = df_list_processed

                    else:
                        # get this table using other library
                        print('couldnt extract this table')
                        # df=plumber.extract_table_by_pageno(pdfPath,i)
                    # print(df)
                # heads = list(filter(None, list(df)))


        except RuntimeError:
            print("There could be NAN values ...couldn;t parse headers")

    #         #df.to_csv('{0}{1}.csv'.format(temp_dir, i+1),index=False)
    #         df.to_json('{0}{1}.json'.format(temp_dir, i+1),orient='records')
    #         #print(df.columns.values)
    #         zipf.write('{0}{1}.json'.format(temp_dir,i+1))
    # # delete all files from temp directory
    # for f in os.listdir(temp_dir):
    #     os.remove(os.path.join(temp_dir,f))
    return page_df_processed


if __name__ == '__main__':
    #df_list = getTablesFromPdf('D:\\DRA\\Analysis of code\\Python\\azure\\sectionextract\\WoodMac_01_Jun_2018_Avocette_and_Coucal.pdf')
    #df_list = getTablesFromPdf('D:\\DRA\\Analysis of code\\Python\\azure\\sectionextract\\Capital-Power (003).pdf', config_dict)
    df_list = getTablesFromPdf('D:\\DRA\\Analysis of code\\Python\\azure\\sectionextract\\Sample4.pdf', config_dict)
    #df_list = getTablesFromPdf('D:\\DRA\\Analysis of code\\Python\\azure\\sectionextract\\MultiplelinesSingleRow.pdf')
    table_json = Utils.export_combined_json(df_list, config_dict)
    #table_json = Utils.export_json(df_list)
