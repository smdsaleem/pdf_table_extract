import pandas as pd
import pdfplumber
from PyPDF2 import PdfFileReader

import Utils

sentences = []
config_dict = {}

def extract_table_by_page(pdf, noOfPages):
    # print(noOfPages)
    first_page = pdf.pages[noOfPages]
    df_list = []
    try:
        tables = first_page.find_tables()
        if tables:
            tables = first_page.extract_tables()

            for table in tables:
                #print('table is ',table)
                df = pd.DataFrame(table[1:], columns=table[0])
                #df = pd.DataFrame(table)
                #df2 = df[df != '']                
                #df2.dropna(axis=0, inplace=True, how='all')
                #df2_columns.dropna(axis=0, inplace=True, how='all')
                #print ("Filtered DF2 Columns: ", df2_columns)
                #print ("Is Empty DF2 Columns: ", df2_columns.empty)
#                if not df2.empty:
#                    #print ("Before DF Data: \n", df2)
#                    df2.dropna(axis=1, inplace=True, how='all')
                df_list.append(df)
                    #df_columns = table[0]
                    #print ("DF Columns: \n", df_columns)
                    
                    #print ("After DF Data: \n", df2)
            #df.to_csv('test.csv')
        return df_list
    except RuntimeError:
        print("Error")


def extract_table_by_pageno(filepath, noOfPages):
    with pdfplumber.open(filepath) as pdf:
        first_page = pdf.pages[noOfPages]
        try:
            tables = first_page.find_tables()
            if tables:
                table = first_page.extract_table()
                # print('table is ',table)
                df = pd.DataFrame(table[1:], columns=table[0])
                # df.to_csv('test.csv')
                if df is not None:
                    heads = list(filter(None, list(df)))
                    #print("head columns **** =", heads)
                    #df.dropna(axis=1, inplace=True, how='all')
#                    if (len(df.columns) == len(heads)):
#                        df.columns = heads
#                    else:
#                        df = None
                pdf.close()
                return df
        except RuntimeError:
            print("Error")


def get_no_pages(filepath):
    pdf = PdfFileReader(open(filepath, 'rb'))
    return pdf.getNumPages()


def extract_using_pdftables(filepath):
    pageNo = get_no_pages(filepath)
    page_df_processed = {}

    with pdfplumber.open(filepath) as pdf:
        for i in range(pageNo):
            print ("On Page:", i+1)
            df_list_processed = []
            df_list = extract_table_by_page(pdf, i)
            if df_list:
                for df in df_list:
                    if df is not None:
                        df = df.fillna(" ")
                        #heads = list(filter(None, list(df)))
                        heads = [x for x in list(df.columns) ]
                        print("head columns ## ", heads)
                        #df.dropna(axis=1, inplace=True, how='all')
                        if (len(df.columns) == len(heads)):
                        #To revert, for testing
                        #if (1 == 1):    
                            df.columns = heads
                            # print(df)
                            df.reset_index()
                            if len(df.index) > 1:
                                df_list_processed.append(df)
                                page_df_processed[i] = df_list_processed

    pdf.close()
    # sentences=generate_sentences(df)
    return page_df_processed


def generate_sentences(df):
    heads = list(filter(None, list(df)))
    df.dropna(axis=1, inplace=True, how='all')

    for row in df.iterrows():
        sentence = ''
        # print(row[1])
        for ite, col in zip(row[1], heads):
            sentence = sentence + " " + (str(col) + ' is ' + str(ite))
        sentences.append(sentence)
    return sentences


if __name__ == '__main__':
    #df_list = extract_using_pdftables('D:\\DRA\\Analysis of code\\Python\\azure\\sectionextract\\WoodMac_26_Jun_2018_Pyeongtaek_LNG_regas_terminal.pdf')
    #df_list = extract_using_pdftables('D:\\DRA\\Analysis of code\\Python\\azure\\sectionextract\\WoodMac_01_Jun_2018_Avocette_and_Coucal.pdf')
    #df_list = extract_using_pdftables('D:\\DRA\\Analysis of code\\Python\\azure\\sectionextract\\Capital-Power (003).pdf')
    df_list = extract_using_pdftables('D:\\DRA\\Analysis of code\\Python\\azure\\sectionextract\\Sample4.pdf')
    #Utils.export_json(df_list)
    Utils.export_combined_json(df_list, config_dict)

    # print(sentences)
