"""
run
~~~
Run the unstructured data pipeline.
"""
from unstructured_data_pipeline.concrete_file_preprocessors import EmlParser, FileGenerator
from unstructured_data_pipeline.configs.config_personal_umbrella import pu_config
from unstructured_data_pipeline.configs.config_dms_claims import dms_config
from unstructured_data_pipeline.configs.config_sa_claims import sa_config

def pu_run_eml_parser():
    print('starting')
    eml_parser = EmlParser(project='personal_umbrella')
    eml_generator = FileGenerator(file_path=pu_config.get_raw_data, file_ext='eml')
    eml_iterator = eml_generator.__iter__()
    print(eml_parser.__dict__)
    counter = 0

    # while True:
    #     try:
    #         eml_parser.extract_text(next(eml_iterator))
    #         counter += 1
    #         print(f"counter: {counter}")
    #     except StopIteration:
    #         print(f"finished processing eml files.")
    #         break
    #
    # # view the contents of the eml mapping dict
    # pprint(eml_parser.mapping_dict)




def main():
    pu_run_eml_parser()


if __name__ == '__main__':
    main()
