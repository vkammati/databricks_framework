from datta_pipeline_library.core.base_config import BaseConfig

BASE_CONFIG_1 = {
    "env": "tst",
    "spn_client_id": "my-spn-client-id",
    "spn_client_secret": "my-spn-client-secret",
    "tenant_id": "my-tenant-id",
    "edc_user_id": "my-edc-user-id",
    "edc_user_pwd": "my-edc-user-name",
    "azure_connection_string": "my-azure-conn-string",
    "json_string": {
        "key_1": "value_1"
    },
    "storage_account": "azdna312eunadlslifwiyswa",
    "catalog_prefix": "cross_ds",
    "project_name": "adf",
    "aecorsoft_project_name": "aecorsoft",
    "datta_subfolder_name":"DS_HANA_CDD",
    "aecorsoft_datta_subfolder_name":"GSAP_ECC_C94",
    "bw_datta_subfolder_name":"DS_GSAP_BW",
    "app_name": "ds",
    "data_governance_domain": "crossds",
    "data_area": "customer_finance_product_asset_cp",
    "data_topology_group": "DS-CrossDS",
    "use_case": "fcb",
    "business_domain": "CROSS_DS",
    "access_grp": {
        "tbl_owner_grp": "owner_grp",
        "tbl_read_grp": "read_grp"
    }
}


class TestGetDatasetConfig:
    def test_confidentiality(self):
        config = BaseConfig(**BASE_CONFIG_1)
        dataset_name = "BKPF"
        actual_dataset_config = config.get_dataset_config(dataset_name, confidentiality=True)

        expected_dataset_config = {
            'bw_landing_data_path': 'abfss://landing@azdna312eunadlslifwiyswa.dfs.core.windows.net/confidential/adf_tst/DS_GSAP_BW/BKPF',
            'bw_landing_view_name': 'bw_view_BKPF',
            'aecorsoft_landing_data_path': 'abfss://landing@azdna312eunadlslifwiyswa.dfs.core.windows.net/confidential/aecorsoft_tst/GSAP_ECC_C94/BKPF/BKPF_None',
            'aecorsoft_landing_view_name': 'aecorsoft_view_BKPF',
            "landing_view_name": "view_BKPF",
            "landing_data_path": "abfss://landing@azdna312eunadlslifwiyswa.dfs.core.windows.net/confidential/adf_tst/DS_HANA_CDD/BKPF/BKPF_None",
            "raw_container_name": "raw",
            "raw_table_name": "raw_BKPF",
            "raw_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/raw/ds_tst/DS_HANA_CDD/BKPF/",
            "archive_raw_table_name": "Aecorsoft_BKPF_archive",
            "archive_raw_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/raw/ds_tst/DS_HANA_CDD/Aecorsoft_BKPF_archive/",
            "euh_table_name": "euh_BKPF",
            "euh_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/enriched-unharmonized/ds_tst/BKPF/",
            "curated_table_name": "curated_BKPF",
            "curated_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/curated/fcb_tst/BKPF/",
            "aecorsoft_raw_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/raw/ds_tst/DS_HANA_CDD/Aecorsoft_BKPF/",
            "curated_src_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/curated/fcb_tst/BKPF/",
            "curated_src_table_name": "curated_BKPF",
            "eh_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/enriched-harmonized/crossds/customer_finance_product_asset_cp_tst/BKPF/",
            "eh_table_name": "eh_BKPF",
            "gx_data_docs_path": "ds_tst/BKPF/docs",
            "gx_metadata_path": "ds_tst/BKPF_great_expectations"
        }

        assert actual_dataset_config == expected_dataset_config

    def test_non_confidentiality(self):
        config = BaseConfig(**BASE_CONFIG_1)
        dataset_name = "CEPCT"
        actual_dataset_config = config.get_dataset_config(dataset_name, confidentiality=False)

        expected_dataset_config = {
            'bw_landing_data_path': 'abfss://landing@azdna312eunadlslifwiyswa.dfs.core.windows.net/internal/adf_tst/DS_GSAP_BW/CEPCT',
            'bw_landing_view_name': 'bw_view_CEPCT',
            'aecorsoft_landing_data_path': 'abfss://landing@azdna312eunadlslifwiyswa.dfs.core.windows.net/internal/aecorsoft_tst/GSAP_ECC_C94/CEPCT/CEPCT_None',
            'aecorsoft_landing_view_name': 'aecorsoft_view_CEPCT',
            "landing_view_name": "view_CEPCT",
            "landing_data_path": "abfss://landing@azdna312eunadlslifwiyswa.dfs.core.windows.net/internal/adf_tst/DS_HANA_CDD/CEPCT/CEPCT_None",
            "raw_container_name": "raw",
            "raw_table_name": "raw_CEPCT",
            "raw_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/raw/ds_tst/DS_HANA_CDD/CEPCT/",
            "archive_raw_table_name": "Aecorsoft_CEPCT_archive",
            "archive_raw_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/raw/ds_tst/DS_HANA_CDD/Aecorsoft_CEPCT_archive/",
            "euh_table_name": "euh_CEPCT",
            "euh_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/enriched-unharmonized/ds_tst/CEPCT/",
            "eh_table_name": "eh_CEPCT",
            "eh_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/enriched-harmonized/crossds/customer_finance_product_asset_cp_tst/CEPCT/",
            "curated_table_name": "curated_CEPCT",
            "curated_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/curated/fcb_tst/CEPCT/",
            "gx_metadata_path": "ds_tst/CEPCT_great_expectations",
            "gx_data_docs_path": "ds_tst/CEPCT/docs",
            "aecorsoft_raw_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/raw/ds_tst/DS_HANA_CDD/Aecorsoft_CEPCT/",
            "curated_src_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/curated/fcb_tst/CEPCT/",
            "curated_src_table_name": "curated_CEPCT",
            "eh_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/enriched-harmonized/crossds/customer_finance_product_asset_cp_tst/CEPCT/"
        }

        assert actual_dataset_config == expected_dataset_config

    def test_non_confidentiality_with_unique_id(self):
        config = BaseConfig(**BASE_CONFIG_1)
        config.set_unique_id("sede-x-DATTA-FI-EUH-feature-fi_euh")
        config.set_unique_id_schema("sede-x-DATTA-FI-EUH-feature-fi_euh")
        dataset_name = "CEPCT"
        actual_dataset_config = config.get_dataset_config(dataset_name, confidentiality=False)

        expected_dataset_config = {
            'bw_landing_data_path': 'abfss://landing@azdna312eunadlslifwiyswa.dfs.core.windows.net/internal/adf_tst/DS_GSAP_BW/sede-x-DATTA-FI-EUH-feature-fi_euh/CEPCT',
            'bw_landing_view_name': 'bw_view_CEPCT',
            'aecorsoft_landing_data_path': 'abfss://landing@azdna312eunadlslifwiyswa.dfs.core.windows.net/internal/aecorsoft_tst/GSAP_ECC_C94/sede-x-DATTA-FI-EUH-feature-fi_euh/CEPCT/CEPCT_None',
            'aecorsoft_landing_view_name': 'aecorsoft_view_CEPCT',
            "landing_view_name": "view_CEPCT",
            "landing_data_path": "abfss://landing@azdna312eunadlslifwiyswa.dfs.core.windows.net/internal/adf_tst/DS_HANA_CDD/sede-x-DATTA-FI-EUH-feature-fi_euh/CEPCT/CEPCT_None",
            "raw_container_name": "raw",
            "raw_table_name": "raw_CEPCT",
            "raw_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/raw/ds_tst/DS_HANA_CDD/sede-x-DATTA-FI-EUH-feature-fi_euh/CEPCT/",
            "archive_raw_table_name": "Aecorsoft_CEPCT_archive",
            "archive_raw_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/raw/ds_tst/DS_HANA_CDD/sede-x-DATTA-FI-EUH-feature-fi_euh/Aecorsoft_CEPCT_archive/",
            "euh_table_name": "euh_CEPCT",
            "euh_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/enriched-unharmonized/ds_tst/sede-x-DATTA-FI-EUH-feature-fi_euh/CEPCT/",
            "eh_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/enriched-harmonized/crossds/customer_finance_product_asset_cp_tst/sede-x-DATTA-FI-EUH-feature-fi_euh/CEPCT/",
            "eh_table_name": "eh_CEPCT",
            "curated_table_name": "curated_CEPCT",
            "curated_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/curated/fcb_tst/sede-x-DATTA-FI-EUH-feature-fi_euh/CEPCT/",
            "gx_data_docs_path": "ds_tst/sede-x-DATTA-FI-EUH-feature-fi_euh/CEPCT/docs",
            "gx_metadata_path": "ds_tst/sede-x-DATTA-FI-EUH-feature-fi_euh/CEPCT_great_expectations",
            "aecorsoft_raw_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/raw/ds_tst/DS_HANA_CDD/sede-x-DATTA-FI-EUH-feature-fi_euh/Aecorsoft_CEPCT/",
            "curated_src_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/curated/fcb_tst/sede-x-DATTA-FI-EUH-feature-fi_euh/CEPCT/",
            "curated_src_table_name": "curated_CEPCT"
        }

        assert actual_dataset_config == expected_dataset_config


class TestUcRelatedFeatures:

    def test_get_tbl_owner_grp(self):
        config = BaseConfig(**BASE_CONFIG_1)
        actual = config.get_tbl_owner_grp()
        expected = "owner_grp"
        assert actual == expected

    def test_get_tbl_read_grp(self):
        config = BaseConfig(**BASE_CONFIG_1)
        actual = config.get_tbl_read_grp()
        expected = "read_grp"
        assert actual == expected

    def test_get_uc_catalog_name_without_unique_id(self):
        config = BaseConfig(**BASE_CONFIG_1)

        actual_catalog = config.get_uc_catalog_name()
        expected_catalog = "cross_ds-unitycatalog-tst"
        assert actual_catalog == expected_catalog

    def test_get_uc_catalog_name_with_unique_id(self):
        config = BaseConfig(**BASE_CONFIG_1)
        config.set_unique_id("sede-x-DATTA-FI-EUH-feature-fi_euh")

        actual_catalog = config.get_uc_catalog_name()
        expected_catalog = "cross_ds-unitycatalog-tst"
        assert actual_catalog == expected_catalog

    def test_format_uc_name(self):
        config = BaseConfig(**BASE_CONFIG_1)

        assert config.format_uc_name("Catalog") == "catalog"
        assert config.format_uc_name("CATALOG") == "catalog"
        assert config.format_uc_name("catalog") == "catalog"
        assert config.format_uc_name("Release-v0.0.1") == "release-v0_0_1"

    def test_get_uc_raw_schema(self):
        config = BaseConfig(**BASE_CONFIG_1)
        actual_uc_raw_schema = config.get_uc_raw_schema()
        expected_uc_raw_schema = "raw-ds"
        assert actual_uc_raw_schema == expected_uc_raw_schema

    def test_get_uc_raw_schema_with_unique_id(self):
        config = BaseConfig(**BASE_CONFIG_1)
        config.set_unique_id_schema("sede-x-DATTA-FI-EUH-feature-fi_euh")
        actual_uc_raw_schema = config.get_uc_raw_schema()
        expected_uc_raw_schema = "raw-ds-sede-x-datta-fi-euh-feature-fi_euh"
        assert actual_uc_raw_schema == expected_uc_raw_schema

    def test_get_uc_euh_schema(self):
        config = BaseConfig(**BASE_CONFIG_1)
        actual_uc_euh_schema = config.get_uc_euh_schema()
        expected_uc_euh_schema = "euh-ds"
        assert actual_uc_euh_schema == expected_uc_euh_schema

    def test_get_uc_euh_schema_with_unique_id(self):
        config = BaseConfig(**BASE_CONFIG_1)
        config.set_unique_id_schema("sede-x-DATTA-FI-EUH-feature-fi_euh")
        actual_uc_euh_schema = config.get_uc_euh_schema()
        expected_uc_euh_schema = "euh-ds-sede-x-datta-fi-euh-feature-fi_euh"
        assert actual_uc_euh_schema == expected_uc_euh_schema

    def test_get_uc_eh_schema(self):
        config = BaseConfig(**BASE_CONFIG_1)
        actual_uc_eh_schema = config.get_uc_eh_schema()
        expected_uc_eh_schema = "eh-crossds-customer_finance_product_asset_cp"
        assert actual_uc_eh_schema == expected_uc_eh_schema

    def test_get_uc_eh_schema_with_unique_id(self):
        config = BaseConfig(**BASE_CONFIG_1)
        config.set_unique_id_schema("sede-x-DATTA-FI-EUH-feature-fi_euh")
        actual_uc_eh_schema = config.get_uc_eh_schema()
        expected_uc_eh_schema = "eh-crossds-customer_finance_product_asset_cp-sede-x-datta-fi-euh-feature-fi_euh"
        assert actual_uc_eh_schema == expected_uc_eh_schema

    def test_get_uc_curated_schema(self):
        config = BaseConfig(**BASE_CONFIG_1)
        actual_uc_curated_schema = config.get_uc_curated_schema()
        expected_uc_curated_schema = "curated-fcb"
        assert actual_uc_curated_schema == expected_uc_curated_schema

    def test_get_uc_curated_schema_with_unique_id(self):
        config = BaseConfig(**BASE_CONFIG_1)
        config.set_unique_id_schema("sede-x-DATTA-FI-EUH-feature-fi_euh")
        actual_uc_curated_schema = config.get_uc_curated_schema()
        expected_uc_curated_schema = "curated-fcb-sede-x-datta-fi-euh-feature-fi_euh"
        assert actual_uc_curated_schema == expected_uc_curated_schema
