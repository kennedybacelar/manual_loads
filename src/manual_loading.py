import pandas as pd 
import sys
import os
import warnings
from datetime import datetime, date

sys.path.insert(1, '../log')
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

def getting_destination_path():

    destination_path = input('Please paste the path where files will be placed\n').replace('\\', '/')
    return (True, [destination_path])

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
        df_dist_names['Dist_key'] = df_dist_names['Distributor_id']
    except Exception as error:
        print('{} - Error sanitizing_dist_names'.format(error))
        return (False, [])
    
    return (True, [df_dist_names])


def getting_corrected_countries(df_automation, df_dist_names):

    try:
        df_automation['temp_country_key'] = df_automation['Country']
        df_automation['temp_dist_key'] = df_automation['Distributor_id']
        df_automation.set_index(['temp_country_key', 'temp_dist_key'], inplace=True)

        df_dist_names.set_index(['Country_key', 'Dist_key'], inplace=True)
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
            print('{}: {} - Distributor not Found Cod: 02'.format(error, single_key_automation_dist_and_country))
            not_valid_distributors.append(single_key_automation_dist_and_country)
        except Exception as error:
            print('{} - Error getting_corrected_countries'.format(error))
            return (False, [])
    
    df_automation.reset_index(drop=True, inplace=True)
    df_dist_names.reset_index(drop=True, inplace=True)
    return (True, [df_automation, valid_automation_distributors, not_valid_distributors])


def removing_invalid_keys_of_df_automation(df_automation, not_valid_distributors):

    try:
        df_automation['temp_country_key'] = df_automation['Country']
        df_automation['temp_dist_key'] = df_automation['Distributor_id']
        df_automation.set_index(['temp_country_key', 'temp_dist_key'], inplace=True)
    except Exception as error:
        print('{} - Error removing_invalid_keys_of_df_automation: Cod01')
        return (False, [])

    try:
        indexes_to_be_removed = pd.MultiIndex.from_tuples(not_valid_distributors)
        df_automation.drop(indexes_to_be_removed, inplace=True)
    except Exception as error:
        print('{} - Error removing_invalid_keys_of_df_automation'.format(error))
        return (False, [])
    
    df_automation.reset_index(drop=True, inplace=True)
    return (True, [df_automation])


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
        df_sales['Store code'] = df_automation_sales['Store_Number']
        df_sales['Product Code'] = df_automation_sales['Chain_Product_Code']
        df_sales['Quantity'] = df_automation_sales['Quantity']
        df_sales['Total Amount WITHOUT TAX'] = df_automation_sales['Sales_Without_Tax']
        df_sales['Total Amount WITH TAX'] = df_automation_sales['Sales_Without_Tax']
    except Exception as error:
        print('{} - error assigning_df_automation_to_df_sales'.format(error))
        return (False, [])

    return (True, [df_sales])


def assigning_df_automation_to_df_stock(df_automation_stock, df_stock):

    try:
        df_stock['Country'] = df_automation_stock['Country']
        df_stock['Diageo Customer ID'] = df_automation_stock['Distributor_id']
        df_stock['Invoice Date'] = df_automation_stock['Invoice_Date']
        df_stock['Warehouse Number'] = df_automation_stock['Store_Number']
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

        #df_dist_names are already set and they are ['Country_key', 'Distributor_id']
        df_dist_names = df_dist_names[~df_dist_names.index.duplicated(keep='first')]
    except Exception as error:
        print('{} - Error when filling_sales_information Cod: 01'.format(error))
        return (False, [])

    for single_key_sales in df_sales.index.unique():
        try:
            correct_dist_name = df_dist_names.at[(single_key_sales), 'Distributor_name']
            currency = df_dist_names.at[(single_key_sales), 'Distributor_Currency']

            df_sales.loc[(single_key_sales), 'Diageo Customer Name'] = correct_dist_name
            df_sales.loc[(single_key_sales), 'Currency Code'] = currency
        except KeyError as error:
            print('{} - Distributor not found'.format(error))
        except Exception as error:
            print('{} - Error when filling_sales_information Cod: 02'.format(error))
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
            correct_dist_name = df_dist_names.at[(single_key_stock), 'Distributor_name']
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


def generating_new_stores_df(df_automation, df_customer_catalogue, df_new_stores_catalogue):

    try:
        df_customer_catalogue.reset_index(inplace=True)
        df_automation.set_index(['Country', 'Distributor_id', 'Store_Number'], inplace=True)
        df_customer_catalogue.set_index(['Country', 'Distributor_id', 'Store_id'], inplace=True)
    except Exception as error:
        print('{} - Error generating_new_stores_df Cod: 01'.format(error))
        return (False, [])
    
    for single_key_automation in df_automation.index.unique():
        try:
            if single_key_automation not in df_customer_catalogue.index.unique():
                store_country, store_dist_id, store_number = single_key_automation

                df_new_stores_catalogue_lenght = len(df_new_stores_catalogue)

                df_new_stores_catalogue.loc[df_new_stores_catalogue_lenght, 'Country'] = store_country
                df_new_stores_catalogue.loc[df_new_stores_catalogue_lenght, 'SAP_Code'] = store_dist_id
                df_new_stores_catalogue.loc[df_new_stores_catalogue_lenght, 'Store Nbr'] = store_number
        except Exception as error:
            print('{} - Error generating_new_stores_df Cod: 02'.format(error))
            return (False, [])

    df_automation.reset_index(inplace=True)
    return (True, [df_new_stores_catalogue])


def creating_folders(destination_path, valid_automation_distributors):

    try:
        os.chdir(destination_path)
        for single_valid_dist in valid_automation_distributors:
            country, dist_code = single_valid_dist
            folder_name = country + '_' + dist_code
            if not os.path.exists(folder_name):
                os.mkdir(folder_name)
    except Exception as error:
        print('{} - Error creating_folders'.format(error))
        return (False, [])
    
    return (True, [])


def writing_files(df_to_be_written, folder_name, file_name):

    try:
        today_date = datetime.today().strftime("%Y%m%d_%H%M%S")
        final_file_path_name = folder_name + '/' + file_name + '_' + today_date + '.csv' 

        df_to_be_written[df_to_be_written.columns].to_csv(final_file_path_name,
            encoding='mbcs', sep=';', columns=df_to_be_written.columns, index=False)
    except Exception as error:
        print('{} | {} - Error writing_files'.format(file_name, error))
        return (False, [])
    
    return (True, [])


def generating_sales_files(df_sales):

    print(df_sales)
    exit() 



def main():

    try:
        print('getting_destination_path')
        success_getting_destination_path, content_getting_destination_path = getting_destination_path()
    except Exception as error:
        print('{} - Error getting_destination_path'.format(Error))
        sys.exit()
    finally:
        if success_getting_destination_path:
            destination_path = content_getting_destination_path[0]
        
    try:
        print('defining_paths')
        success_defining_paths, content_defining_paths = defining_paths()
    except Exception as error:
        print('{} - Error defining_paths')
        sys.exit()
    finally:
        if success_defining_paths:
            automation_template_path = content_defining_paths[0]
            customer_catalogue_path = content_defining_paths[1]
            dist_names_path = content_defining_paths[2]
            product_master_path = content_defining_paths[3]
            sku_map_path = content_defining_paths[4]
        else:
            sys.exit()

    try:
        print('loading_frames')
        success_loading_frames , content_loading_frames = loading_frames(automation_template_path, customer_catalogue_path, dist_names_path,
            product_master_path, sku_map_path)
    except Exception as error:
        print('{} - Error loading_frames'.format(error))
        sys.exit()
    finally:
        if success_loading_frames:
            df_automation = content_loading_frames[0]
            df_customer_catalogue = content_loading_frames[1]
            df_dist_names = content_loading_frames[2]
            df_sku_map = content_loading_frames[3]
            df_sap_codes_vs_chains = content_loading_frames[4]
        else:
            sys.exit()
        
    try:
        print('declaring_sales_file_final_format')
        success_declaring_sales_file_final_format, content_declaring_sales_file_final_format = declaring_sales_file_final_format()
    except Exception as error:
        print('{} - Error declaring_sales_file_final_format'.format(error))
        sys.exit()
    finally:
        if success_declaring_sales_file_final_format:
            df_sales = content_declaring_sales_file_final_format[0]

    try:
        print('declaring_stock_file_final_format')
        success_declaring_stock_file_final_format , content_declaring_stock_file_final_format = declaring_stock_file_final_format()
    except Exception as error:
        print('{} - Error declaring_stock_file_final_format'.format(error))
        sys.exit()
    finally:
        if success_declaring_stock_file_final_format:
            df_stock = content_declaring_stock_file_final_format[0]
    
    try:
        print('sanitizing_df_automation')
        success_sanitizing_df_automation , content_sanitizing_df_automation = sanitizing_df_automation(df_automation)
    except Exception as error:
        print('{} - Error sanitizing_df_automation'.format(error))
        sys.exit()
    finally:
        if success_sanitizing_df_automation:
            df_automation = content_sanitizing_df_automation[0]
        else:
            sys.exit()

    try:
        print('df_automation_wrong_data_type_column_inputs')
        success_df_automation_wrong_data_type_column_inputs , content_df_automation_wrong_data_type_column_inputs = df_automation_wrong_data_type_column_inputs(df_automation)
    except Exception as error:
        print('{} - Error df_automation_wrong_data_type_column_inputs')
        sys.exit()
    finally:
        if success_df_automation_wrong_data_type_column_inputs:
            df_automation = content_df_automation_wrong_data_type_column_inputs[0]
        else:
            sys.exit()

    try:
        print('sanitizing_df_sap_codes_vs_chains')
        success_sanitizing_df_sap_codes_vs_chains ,content_sanitizing_df_sap_codes_vs_chains = sanitizing_df_sap_codes_vs_chains(df_sap_codes_vs_chains)
    except Exception as error:
        print('{} - Error sanitizing_df_sap_codes_vs_chains'.format(error))
        sys.exit()
    finally:
        if success_sanitizing_df_sap_codes_vs_chains:
            df_sap_codes_vs_chains = content_sanitizing_df_sap_codes_vs_chains[0]
        else:
            sys.exit()
    
    try:
        print('getting_corrected_sap_codes') 
        success_getting_corrected_sap_codes, content_getting_corrected_sap_codes = getting_corrected_sap_codes(df_automation, df_sap_codes_vs_chains)
    except Exception as error:
        print('{} - Error getting_corrected_sap_codes')
        sys.exit()
    finally:
        if success_getting_corrected_sap_codes:
            df_automation = content_getting_corrected_sap_codes[0]
        else:
            sys.exit()
    
    try:
        print('sanitizing_dist_names')
        success_sanitizing_dist_names, content_sanitizing_dist_names = sanitizing_dist_names(df_dist_names)
    except Exception as error:
        print('{} - Error sanitizing_dist_names')
        sys.exit()
    finally:
        if success_sanitizing_dist_names:
            df_dist_names = content_sanitizing_dist_names[0]
        else:
            sys.exit()

    try:
        print('getting_corrected_countries')
        success_getting_corrected_countries, content_getting_corrected_countries = getting_corrected_countries(df_automation, df_dist_names)
    except Exception as error:
        print('{} - Error getting_corrected_countries'.format(error))
        sys.exit()
    finally:
        if success_getting_corrected_countries:
            df_automation = content_getting_corrected_countries[0]
            valid_automation_distributors = content_getting_corrected_countries[1]
            not_valid_distributors = content_getting_corrected_countries[2]
        else:
            sys.exit()
    
    try:
        print('removing_invalid_keys_of_df_automation')
        success_removing_invalid_keys_of_df_automation, content_removing_invalid_keys_of_df_automation = removing_invalid_keys_of_df_automation(df_automation, not_valid_distributors)
    except Exception as error:
        print('{} - Error removing_invalid_keys_of_df_automation'.format(error))
        sys.exit()
    finally:
        if success_removing_invalid_keys_of_df_automation:
            df_automation = content_removing_invalid_keys_of_df_automation[0]
        else:
            sys.exit()

    try:
        print('splitting_sales_and_stock')
        success_splitting_sales_and_stock, content_splitting_sales_and_stock = splitting_sales_and_stock(df_automation)
    except Exception as error:
        print('{} - Error splitting_sales_and_stock'.format(error))
        sys.exit()
    finally:
        if success_splitting_sales_and_stock:
            df_automation_sales = content_splitting_sales_and_stock[0]
            df_automation_stock = content_splitting_sales_and_stock[1]
        else:
            sys.exit()

    try:
        print('assigning_df_automation_to_df_sales')
        success_assigning_df_automation_to_df_sales, content_assigning_df_automation_to_df_sales = assigning_df_automation_to_df_sales(df_automation_sales, df_sales)
    except Exception as error:
        print('{} - Error assigning_df_automation_to_df_sales'.format(error))
        sys.exit()
    finally:
        if success_assigning_df_automation_to_df_sales:
            df_sales = content_assigning_df_automation_to_df_sales[0]
        else:
            sys.exit()
    
    try:
        print('assigning_df_automation_to_df_stock')
        success_assigning_df_automation_to_df_stock, content_assigning_df_automation_to_df_stock = assigning_df_automation_to_df_stock(df_automation_stock, df_stock)
    except Exception as error:
        print('{} - Error assigning_df_automation_to_df_stock')
        sys.exit()
    finally:
        if success_assigning_df_automation_to_df_stock:
            df_stock = content_assigning_df_automation_to_df_stock[0]
        else:
            sys.exit()

    try:
        print('sanitizing_sales_file')
        success_sanitizing_sales_file, content_sanitizing_sales_file = sanitizing_sales_file(df_sales)
    except Exception as error:
        print('{} - Error sanitizing_sales_file'.format(error))
        sys.exit()
    finally:
        if success_sanitizing_sales_file:
            df_sales = content_sanitizing_sales_file[0]
        else:
            sys.exit()
    
    try:
        print('filling_sales_information')
        success_filling_sales_information, content_filling_sales_information = filling_sales_information(df_sales, df_dist_names)
    except Exception as error:
        print('{} - Error filling_sales_information'.format(error))
        sys.exit()
    finally:
        if success_filling_sales_information:
            df_sales = content_filling_sales_information[0]
        else:
            sys.exit()
    
    try:
        print('filling_stock_information')
        success_filling_stock_information, content_filling_stock_information = filling_stock_information(df_stock, df_dist_names, df_customer_catalogue)
    except Exception as error:
        print('{} - Error filling_stock_information'.format(error))
        sys.exit()
    finally:
        if success_filling_stock_information:
            df_stock = content_filling_stock_information[0]
        else:
            sys.exit()
    
    try:
        print('creating_new_stores_dataframe')
        success_creating_new_stores_dataframe, content_creating_new_stores_dataframe = creating_new_stores_dataframe()
    except Exception as error:
        print('{} - Error creating_new_stores_dataframe')
        sys.exit()
    finally:
        if success_creating_new_stores_dataframe:
            df_new_stores_catalogue = content_creating_new_stores_dataframe[0]

    try:
        print('generating_new_stores_df')
        success_generating_new_stores_df, content_generating_new_stores_df = generating_new_stores_df(df_automation, df_customer_catalogue, df_new_stores_catalogue)
    except Exception as error:
        print('{} - Error generating_new_stores_df')
        sys.exit()
    finally:
        if success_generating_new_stores_df:
            df_new_stores_catalogue = content_generating_new_stores_df[0]
        else:
            sys.exit()

    try:
        print('creating_folders')
        creating_folders(destination_path, valid_automation_distributors)
    except Exception as error:
        print('{} - Error creating_folders')
        sys.exit()

    generating_sales_files(df_sales)
    print(df_automation.head(5))


if __name__ == "__main__":
    main()