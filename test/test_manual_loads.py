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
            'Invoice_Date', 'Store_Number', 'Chain_Product_Code',
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
        df_dist_names['Distributor_id'] = ['276643', '73322']
        df_dist_names['Distributor_country'] = ['Peru', 'Uruguay LS']

        df_expected['Country'] = ['Peru', 'Uruguay LS', 'Egypt']
        df_expected['Distributor_id'] = ['276643', '73322', '123']

        expected_valid_automation_distributors = [('Peru', '276643'), ('Uruguay LS', '73322')]
        expected_not_valid_distributors = [('Egypt', '123')]

        success, content = manual_loading.getting_corrected_countries(df_automation, df_dist_names)
        returned_df_automation = content[0]
        returned_valid_automation_distributors = content[1]
        returned_not_valid_distributors = content[2]

        self.assertEqual(success, True)
        pd.testing.assert_frame_equal(returned_df_automation, df_expected)
        self.assertEqual(returned_valid_automation_distributors, expected_valid_automation_distributors)
        self.assertEqual(returned_not_valid_distributors, expected_not_valid_distributors)

if __name__ == "__main__":
    unittest.main()