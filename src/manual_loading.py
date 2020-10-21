import pandas as pd 
import sys
import os
import warnings

sys.path.insert(1, '../log')
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

def defining_paths():

    automation_template_path = '../Catalogues_Manual_Loads/Automation_Template.xlsx'
    customer_catalogue_path = '../Catalogues_Manual_Loads/customer_catalogue.xlsx'
    dist_names_path = '../Catalogues_Manual_Loads/dist_names.xlsx'
    product_master_path = '../Catalogues_Manual_Loads/product_master.xlsx'
    sku_map_path = '../Catalogues_Manual_Loads/product_master.xlsx'

    return (True, 
            [automation_template_path, 
            customer_catalogue_path,
            dist_names_path,
            product_master_path,
            sku_map_path])
    

def loading_frames(automation_template_path, customer_catalogue_path, dist_names_path,
    product_master_path, sku_map_path):

    try:
        df_automation = pd.read_excel(automation_template_path, dtype=str).fillna('')
    except Exception as error:
        print(error)
        return (False, [])
    
    try:
        df_customer_catalogue = pd.read_excel(customer_catalogue_path, dtype=str).fillna('')
    except Exception as error:
        print(error)
        return (False, [])
    
    try:
        df_dist_names = pd.read_excel(dist_names_path, dtype=str).fillna('')
    except Exception as error:
        print(error)
        return (False, [])
    
    try:
        df_sku_map = pd.read_excel(sku_map_path, dtype=str).fillna('')
    except Exception as error:
        print(error)
        return (False, [])
    
    try:
        df_sap_codes_vs_chains = pd.read_excel(dist_names_path, dtype=str,
            sheet_name='sap_codes_vs_chains').fillna('')
    except Exception as error:
        print(error)
        return (False, [])
    
    return (True, [df_automation, df_customer_catalogue, df_dist_names, df_sku_map, df_sap_codes_vs_chains])


def declaring_sales_file_final_format():

    sales_file_columns = [
        'Country', 'Diageo Customer ID', 'Diageo Customer Name', 'Invoice number', 'Type of Invoice', 
        'Invoice Date', 'Store code', 'Product Code', 'Quantity', 'Unit of measure', 'Total Amount WITHOUT TAX',
        'Total Amount WITH TAX', 'Currency Code', 'Sales Representative Code'

    ]

    df_sales = pd.DataFrame(columns=sales_file_columns).fillna('')

    return (True, [df_sales])


def declaring_stock_file_final_format():

    stock_file_columns = [
        'Country', 'Product Code' 'Diageo Customer ID', 'Diageo Customer Name',
        'Invoice Date', 'Quantity', 'Unit of measure', 'Warehouse Number', 'Warehouse'
    ]

    df_stock = pd.DataFrame(columns=stock_file_columns).fillna('')

    return (True, [df_stock])


def sanitizing_df_automation(df_automation):

    try:
        df_automation['Data_Type'] = df_automation['Data_Type'].str.lower()
        df_automation['Data_Type'] = df_automation['Data_Type'].str.strip()
        df_automation['Distributor_id'] = df_automation['Distributor_id'].str.replace('[^a-zA-Z0-9-áéíóú]+', '')
        df_automation['Distributor_id'] = df_automation['Distributor_id'].str.lower()
        df_automation['Chain_Product_Code'] = df_automation['Chain_Product_Code'].str.lstrip('0')
        df_automation['Store_Number'] = df_automation['Store_Number'].str.lstrip('0')
        df_automation['Store_Number'] = df_automation['Store_Number'].str.strip()
        df_automation['Store_Number'] = df_automation['Store_Number'].str[:12]
        df_automation['Country'] = df_automation['Country'].str.lower()
        df_automation['Country'] = df_automation['Country'].str.strip()
    except KeyError as error:
        print('{} - Column not found'.format(error))
    except Exception as error:
        print(error)
        return (False, [])
    
    return (True, [df_automation])


def df_automation_wrong_data_type_column_inputs(df_automation):

    try:
        filter_wrong_data = df_automation[(df_automation['Data_Type'] != 'sales') & (df_automation['Data_Type'] != 'stock')].index
        df_automation.drop(filter_wrong_data, inplace=True)
    except Exception as error:
        print(error)
        return (False, [])
    
    return (True, [df_automation])


def sanitizing_df_sap_codes_vs_chains(df_sap_codes_vs_chains):

    try:
        df_sap_codes_vs_chains['Distributor_alias'] = df_sap_codes_vs_chains['Distributor_alias'].str.replace('[^a-zA-Z0-9-áéíóú]+', '')
        df_sap_codes_vs_chains['Distributor_alias'] = df_sap_codes_vs_chains['Distributor_alias'].str.lower()
    except Exception as error:
        print(error)
        return (False, [])
    
    return (True, [df_sap_codes_vs_chains])


def getting_corrected_sap_codes(df_automation, df_sap_codes_vs_chains):

    df_automation['temporary_key'] = df_automation['Distributor_id']
    df_automation.set_index(['temporary_key'], inplace=True)

    df_sap_codes_vs_chains['temporary_key'] = df_sap_codes_vs_chains['Distributor_alias']
    df_sap_codes_vs_chains.set_index(['temporary_key'], inplace=True)
    df_sap_codes_vs_chains = df_sap_codes_vs_chains[~df_sap_codes_vs_chains.index.duplicated(keep='first')]

    not_found_dist_ids = list()

    for single_key in df_automation.index.unique():
        try:
            corrected_dist_code = df_sap_codes_vs_chains.at[(single_key), 'Distributor_id']
            df_automation.loc[(single_key), 'Distributor_id'] = corrected_dist_code
        except KeyError as error:
            not_found_dist_ids.append(error)
            print('{} - Corresponding distributor not found'.format(error))
        except Exception as error:
            print(error)
        
    df_automation.reset_index(drop=True, inplace=True)
    df_sap_codes_vs_chains.reset_index(drop=True, inplace=True)
        
    return (True, [df_automation, not_found_dist_ids])


def sanitizing_dist_names(df_dist_names):

    try:
        df_dist_names['Distributor_country'] = df_dist_names['Distributor_country'].str.strip()
        df_dist_names['Distributor_name'] = df_dist_names['Distributor_name'].str.strip()
        df_dist_names['Distributor_Currency'] = df_dist_names['Distributor_Currency'].str.strip()
        df_dist_names['Distributor_id'] = df_dist_names['Distributor_id'].str.strip()

        #Auxiliar column to be used as key
        df_dist_names['Country_key'] = df_dist_names['Distributor_country'].str.lower()
    except Exception as error:
        print('{} - Error sanitizing_dist_names'.format(error))
        return (False, [])
    
    return (True, [df_dist_names])


def getting_corrected_countries(df_automation, df_dist_names):

    try:
        df_automation['temp_country_key'] = df_automation['Country']
        df_automation['temp_dist_key'] = df_automation['Distributor_id']
        df_automation.set_index(['temp_country_key', 'temp_dist_key'], inplace=True)

        df_dist_names.set_index(['Country_key', 'Distributor_id'], inplace=True)
        df_dist_names = df_dist_names[~df_dist_names.index.duplicated(keep='first')]
    except Exception as error:
        print('{} - Error getting_corrected_countries - Cod: 01'.format(error))
        return (False, [])

    valid_automation_distributors = list()
    not_valid_distributors = list()

    for single_key_automation_dist_and_country in df_automation.index.unique():
        try:
            correct_dist_country = df_dist_names.at[(single_key_automation_dist_and_country), 'Distributor_country']
            df_automation.loc[(single_key_automation_dist_and_country), 'Country'] = correct_dist_country
            valid_automation_distributors.append((correct_dist_country, single_key_automation_dist_and_country[1]))
        except KeyError as error:
            print('{} - Distributor not Found'.format(error))
            not_valid_distributors.append(single_key_automation_dist_and_country)
        except Exception as error:
            print('{} - Error getting_corrected_countries'.format(error))
            return (False, [])
    
    df_automation.reset_index(drop=True, inplace=True)
    return (True, [df_automation, valid_automation_distributors, not_valid_distributors])
    

def splitting_sales_and_stock(df_automation):

    try:
        filt_sales = df_automation['Data_Type'] == 'sales'
        df_automation_sales = df_automation[filt_sales]

        filt_stock = df_automation['Data_Type'] == 'stock'
        df_automation_stock = df_automation[filt_stock]
    except Exception as error:
        print(error)
        return (False, [])
    
    return (True, [df_automation_sales, df_automation_stock])


def assigning_df_automation_to_df_sales(df_automation_sales, df_sales):

    try:
        df_sales['Country'] = df_automation_sales['Country']
        df_sales['Diageo Customer ID'] = df_automation_sales['Distributor_id']
        df_sales['Invoice Date'] = df_automation_sales['Invoice_Date']
        df_sales['Store code'] = df_automation_sales['Store_Code']
        df_sales['Product Code'] = df_automation_sales['Chain_Product_Code']
        df_sales['Quantity'] = df_automation_sales['Quantity']
        df_sales['Total Amount WITHOUT TAX'] = df_automation_sales['Sales_Without_Tax']
        df_sales['Total Amount WITH TAX'] = df_automation_sales['Sales_Without_Tax']
    except Exception as error:
        print('{} - error assigning_df_automation_to_df_sales'.format(error))
        return (False, [])

    return (True, df_sales)


def assigning_df_automation_to_df_stock(df_automation_stock, df_stock):

    try:
        df_stock['Country'] = df_automation_stock['Country']
        df_stock['Diageo Customer ID'] = df_automation_stock['Distributor_id']
        df_stock['Invoice Date'] = df_automation_stock['Invoice_Date']
        df_stock['Warehouse Number'] = df_automation_stock['Store_Code']
        df_stock['Product Code'] = df_automation_stock['Chain_Product_Code']
        df_stock['Quantity'] = df_automation_stock['Quantity']
    except Exception as error:
        print('{} - Error assigning_df_automation_to_df_stock'.format(error))
        return (False, [])

    return (True, [df_stock])


def sanitizing_sales_file(df_sales):
    
    try:
        #Removing negative sign from the end of the values (Some samples were found)
        values_that_end_with_negative_sign_quantity = (df_sales['Quantity'].str[-1] == '-')
        df_sales.loc[values_that_end_with_negative_sign_quantity, 'Quantity'] = '-' + df_sales.loc[values_that_end_with_negative_sign_quantity, 'Quantity'].str[:-1]
        
        values_that_end_with_negative_sign_total_with_tax = (df_sales['Total Amount WITH TAX'].str[-1] == '-')
        df_sales.loc[values_that_end_with_negative_sign_total_with_tax, 'Total Amount WITH TAX'] = '-' + df_sales.loc[values_that_end_with_negative_sign_total_with_tax, 'Total Amount WITH TAX'].str[:-1]
        
        values_that_end_with_negative_sign_total_without_tax = (df_sales['Total Amount WITHOUT TAX'].str[-1] == '-')
        df_sales.loc[values_that_end_with_negative_sign_total_without_tax, 'Total Amount WITHOUT TAX'] = '-' + df_sales.loc[values_that_end_with_negative_sign_total_without_tax, 'Total Amount WITHOUT TAX'].str[:-1]
        
        #Turning it numeric below quantities
        df_sales['Quantity'] = pd.to_numeric(df_sales['Quantity']).fillna(0)
        df_sales['Total Amount WITH TAX'] = pd.to_numeric(df_sales['Total Amount WITH TAX']).fillna(0)
        df_sales['Total Amount WITHOUT TAX'] = pd.to_numeric(df_sales['Total Amount WITHOUT TAX']).fillna(0)
    
        df_sales = df_sales.fillna('')
    except Exception as error:
        print(error)
        return (False, [])

    return (True, [df_sales])
        

def filling_sales_information(df_sales, df_dist_names):
    
    try:
        df_sales['temp_country_key'] = df_sales['Country'].str.lower()
        df_sales['temp_dist_key'] = df_sales['Diageo Customer ID']
        df_sales.set_index(['temp_country_key', 'temp_dist_key'], inplace=True)
        df_sales = df_sales[~df_sales.index.duplicated(keep='first')]

        df_dist_names['temp_country_key'] = df_dist_names['Distributor_country']
        df_dist_names['temp_dist_key'] = df_dist_names['Distributor_id']
        df_dist_names.set_index(['temp_country_key', 'temp_dist_key'], inplace=True)
        df_dist_names = df_dist_names[~df_dist_names.index.duplicated(keep='first')]
    except Exception as error:
        print('{} - Error when filling_sales_information'.format(error))
        return (False, [])

    for single_key_sales in df_sales.index.unique():
        try:
            correct_dist_country = df_dist_names.at[(single_key_sales), 'Distributor_country']
            correct_dist_name = df_dist_names.at[(single_key_sales), 'Distributor_name']
            currency = df_dist_names.at[(single_key_sales), 'Distributor_Currency']

            df_sales.loc[(single_key_sales), 'Country'] = correct_dist_country
            df_sales.loc[(single_key_sales), 'Diageo Customer Name'] = correct_dist_name
            df_sales.loc[(single_key_sales), 'Currency Code'] = currency
        except KeyError as error:
            print('{} - Distributor not found'.format(error))
        except Exception as error:
            print('{} - Error when filling_sales_information'.format(error))
            return (False, [])

    #Keeping the dist names and sales with index set - So not needed setting index again

    return (True, [df_sales])


def filling_stock_information(df_stock, df_dist_names, df_customer_catalogue):

    try:
        df_customer_catalogue.set_index(['Distributor_id', 'Store_id'], inplace=True)
        df_customer_catalogue = df_customer_catalogue[~df_customer_catalogue.index.duplicated(keep='first')]

        df_stock['temp_country_key'] = df_stock['Country'].str.lower()
        df_stock['temp_dist_key'] = df_stock['Diageo Customer ID']
        df_stock.set_index(['temp_country_key', 'temp_dist_key'], inplace=True)
        df_stock = df_stock[~df_stock.index.duplicated(keep='first')]
    except Exception as error:
        print('{} - Error filling_stock_information'.format(error))
        return (False, [])

    for single_key_stock in df_stock.index.unique():
        try:
            correct_dist_country = df_dist_names.at[(single_key_stock), 'Distributor_country']
            correct_dist_name = df_dist_names.at[(single_key_stock), 'Distributor_name']

            df_stock.loc[(single_key_stock), 'Country'] = correct_dist_country
            df_stock.loc[(single_key_stock), 'Diageo Customer Name'] = correct_dist_name
        except KeyError as error:
            print('{} - Distributor not found'.format(error))
        except Exception as error:
            print('{} - Error filling_stock_information'.format(error))
            return (False, [])
        
        try:
            store_name = df_customer_catalogue.at[(single_key_stock), 'Store_name']

            df_stock.loc[(single_key_stock), 'Warehouse'] = store_name
        except KeyError as error:
            print('{} - Store not found'.format(error))
        except Exception as error:
            print(error)
            return (False, [])

    return (True, [df_stock])


def creating_new_stores_dataframe():

    new_stores_catalogue_columns = [
        'POS_ID', 'Store Nbr', 'Store Name', 'SAP_Code', 'Chain', 
        'Commercial Group', 'Store/Business', 'Type', 'Subchannel',
        'Channel', 'Trade', 'Segment', 'Occasion', 'Occasion Segment', 
        'Mechandiser', 'Supervisor', 'Provice or Commune', 'City',
        'State or Region', 'Country', 'COU'
        ]
    df_new_stores_catalogue = pd.DataFrame(columns=new_stores_catalogue_columns)
    return (True, [df_new_stores_catalogue])


    




def main():
    pass

if __name__ == "__main__":
    main()