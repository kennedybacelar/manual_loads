import unittest
import sys
import pandas as pd
sys.path.insert(1, '../src')

import manual_loading

class TestManualLoading(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.automation_template_columns = [
            'Country', 'Distributor_id', 'Data_Type',
            'Invoice_Date', 'Store_Number', 'Store_Name', 'Chain_Product_Code',
            'Quantity', 'Sales_Without_Tax', 'Sales_With_Tax'
            ]

        cls.sales_file_columns = [
            'Country', 'Diageo Customer ID', 'Diageo Customer Name', 'Invoice number', 'Type of Invoice', 
            'Invoice Date', 'Store code', 'Product Code', 'Quantity', 'Unit of measure', 'Total Amount WITHOUT TAX',
            'Total Amount WITH TAX', 'Currency Code', 'Sales Representative Code'

            ]
        
        cls.stock_file_columns = [
            'Country', 'Product Code' 'Diageo Customer ID', 'Diageo Customer Name',
            'Invoice Date', 'Quantity', 'Unit of measure', 'Warehouse Number', 'Warehouse'
            ]

        cls.sap_codes_vs_chains_columns = [
            'Distributor_alias', 'Distributor_id'
            ]
        
        cls.dist_names_columns = [
            'Distributor_country', 'Distributor_id', 'Distributor_name', 'Distributor_Currency'
            ]
        
        cls.sku_map_columns = [
                'Dist_SAP_Code', 'Dist_SKU_Code', 'Dist_Description',
                'Diageo_SKU_Code', 'Multiplication_Factor', 'Share_Participation'
            ]
    

    def test_sanitizing_df_automation(self):
        
        df_automation = pd.DataFrame(columns=self.automation_template_columns)
        df_expected = pd.DataFrame(columns=self.automation_template_columns)

        df_automation['Data_Type'] = ['SALES ', '  sTOCk   ', ' Stock']
        df_automation['Distributor_id'] = ['LONDON SUPPLY S.A.C.I.F.I.', 'H & Cia Srl POS', '250674']

        df_expected['Data_Type'] = ['sales', 'stock', 'stock']
        df_expected['Distributor_id'] = ['londonsupplysacifi', 'hciasrlpos', '250674']

        success, content = manual_loading.sanitizing_df_automation(df_automation)
        df_automation_returned = content[0]

        self.assertEqual(success, True)
        pd.testing.assert_frame_equal(df_automation_returned, df_expected)


    def test_sanitizing_df_sap_codes_vs_chains(self):

        df_sap_codes_vs_chains = pd.DataFrame(columns=self.sap_codes_vs_chains_columns)
        df_expected = pd.DataFrame(columns=self.sap_codes_vs_chains_columns)

        df_sap_codes_vs_chains['Distributor_alias'] = [' INDUSTRIA COMERCIAL EL DORADO S.A.C ', ' TINTOS & HIELOS SCRL ']
        
        df_expected['Distributor_alias'] = ['industriacomercialeldoradosac', 'tintoshielosscrl']

        success, content = manual_loading.sanitizing_df_sap_codes_vs_chains(df_sap_codes_vs_chains)
        returned_df_sap_codes_vs_chains = content[0]

        self.assertEqual(success, True)
        pd.testing.assert_frame_equal(returned_df_sap_codes_vs_chains, df_expected)
    

    def test_getting_corrected_sap_codes(self):

        df_automation = pd.DataFrame(columns=self.automation_template_columns)
        df_sap_codes_vs_chains = pd.DataFrame(columns = self.sap_codes_vs_chains_columns)
        df_expected = pd.DataFrame(columns = self.automation_template_columns)

        df_automation['Distributor_id'] = ['Polakof Y Cia Sa POS', 'T & S Operaciones Logisticas Sac']

        df_sap_codes_vs_chains['Distributor_alias'] = ['Polakof Y Cia Sa POS', 'T & S Operaciones Logisticas Sac']
        df_sap_codes_vs_chains['Distributor_id'] = ['255682', '269683']

        df_expected['Distributor_id'] = ['255682', '269683']

        success, content = manual_loading.getting_corrected_sap_codes(df_automation, df_sap_codes_vs_chains)
        returned_df = content[0]


        self.assertEqual(success, True)
        pd.testing.assert_frame_equal(returned_df, df_expected)
    

    def test_getting_corrected_countries(self):

        df_automation = pd.DataFrame(columns=self.automation_template_columns)
        df_expected = pd.DataFrame(columns=self.automation_template_columns)
        df_dist_names = pd.DataFrame(columns=self.dist_names_columns)

        df_automation['Country'] = ['peru', 'uruguay ls', 'Egypt']
        df_automation['Distributor_id'] = ['276643', '73322', '123']

        df_dist_names['Country_key'] = ['peru', 'uruguay ls']
        df_dist_names['Dist_key'] = ['276643', '73322']
        df_dist_names['Distributor_country'] = ['Peru', 'Uruguay LS']

        df_expected['Country'] = ['Peru', 'Uruguay LS', 'Egypt']
        df_expected['Distributor_id'] = ['276643', '73322', '123']

        expected_valid_automation_distributors = [('Peru', '276643'), ('Uruguay LS', '73322')]
        expected_not_valid_distributors = [('Egypt', '123')]

        success, content = manual_loading.getting_corrected_countries(df_automation, df_dist_names)
        if success:
            returned_df_automation = content[0]
            returned_valid_automation_distributors = content[1]
            returned_not_valid_distributors = content[2]

        self.assertEqual(success, True)
        pd.testing.assert_frame_equal(returned_df_automation, df_expected)
        self.assertEqual(returned_valid_automation_distributors, expected_valid_automation_distributors)
        self.assertEqual(returned_not_valid_distributors, expected_not_valid_distributors)
    

    def test_removing_invalid_keys_of_df_automation(self):

        df_automation = pd.DataFrame(columns=self.automation_template_columns)
        df_expected  = pd.DataFrame(columns=self.automation_template_columns)
        not_valid_distributors = [('China', '789')]

        df_automation['Country'] = ['Brazil', 'Uruguay', 'China']
        df_automation['Distributor_id'] = ['123', '456', '789']

        df_expected['Country'] = ['Brazil', 'Uruguay']
        df_expected['Distributor_id'] = ['123', '456']

        success, content = manual_loading.removing_invalid_keys_of_df_automation(df_automation, not_valid_distributors)
        if success:
            returned_df_automation = content[0]

        self.assertEqual(True, success)
        pd.testing.assert_frame_equal(returned_df_automation, df_expected)
    

    def test_creating_new_skus_map_dataframe(self):

        df_sku_map = pd.DataFrame(columns=self.sku_map_columns)
        success, content = manual_loading.creating_new_skus_map_dataframe()
        returned_df_sku_map = content[0]

        self.assertEqual(success, True)
        pd.testing.assert_frame_equal(returned_df_sku_map, df_sku_map)

    
    def test_mapping_new_skus(self):

        df_automation = pd.DataFrame(columns=self.automation_template_columns)
        df_sku_map = pd.DataFrame(columns=self.sku_map_columns)
        df_unmapped_skus = pd.DataFrame(columns=self.sku_map_columns)
        df_expected_unmapped_skus = pd.DataFrame(columns=self.sku_map_columns)

        df_automation['Distributor_id'] = ['123', '456']
        df_automation['Chain_Product_Code'] = ['ABC', 'DEF']

        df_sku_map['Distributor_SAP_Code'] = ['123']
        df_sku_map['Distributor_SKU_Code'] = ['ABC']

        df_expected_unmapped_skus['Dist_SAP_Code'] = ['456']
        df_expected_unmapped_skus['Dist_SKU_Code'] = ['DEF']
        df_expected_unmapped_skus['Multiplication_Factor'] = 1
        df_expected_unmapped_skus['Share_Participation'] = 1

        df_expected_automation = df_automation

        success, content = manual_loading.mapping_new_skus(df_automation, df_sku_map, df_unmapped_skus)

        returned_df_automation = content[0]
        returned_df_unmapped_skus = content[1]

        self.assertEqual(success, True)
        pd.testing.assert_frame_equal(returned_df_automation, df_expected_automation)
        pd.testing.assert_frame_equal(returned_df_unmapped_skus, df_expected_unmapped_skus)


if __name__ == "__main__":
    unittest.main()