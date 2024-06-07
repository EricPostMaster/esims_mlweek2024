import yaml

from module_spark.dfops import read_func
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core import ExpectationConfiguration


def run_great_expectations(data, config_path, verbose=False):
    """Run Great Expectations expectation suite on Spark DataFrame from the lake

    Args:
        config_path (str): YAML configuration path
        verbose (bool): whether to print validation result statistics

    Returns:
        ExpectationSuiteValidationResult: validation result object

    """

    df = data

    # Load config - THIS PROBABLY NEEDS TO BE CUSTOMIZED FOR YOUR SITUATION
    with open(config_path, "r", encoding="utf-8") as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            return

    # Add datasource and batch request
    context = gx.get_context()
    context.test_yaml_config(str(config["datasource_config"]))
    context.add_datasource(**config["datasource_config"])

    batch_request = RuntimeBatchRequest(
        datasource_name=config["datasource_config"]["name"],
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=config["batch_request_config"]["data_asset_name"],
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "default_identifier"},
    )

    # Create expectation suite
    context.add_or_update_expectation_suite(
        expectation_suite_name=config["expectation_suite_config"]["expectation_suite_name"],
    )

    suite = context.get_expectation_suite(
        expectation_suite_name=config["expectation_suite_config"]["expectation_suite_name"]
    )

    for expectation in config["expectations"]:
        for kwarg in expectation["kwargs"]:
            suite.add_expectation(
                expectation_configuration=ExpectationConfiguration(
                    expectation_type=expectation["expectation_type"], kwargs=kwarg
                )
            )

    # Validate
    validator = context.get_validator(expectation_suite=suite, batch_request=batch_request)
    result = validator.validate(expectation_suite=suite)

    # Validation results
    if verbose:
        print(result["statistics"])
        print(result)

    return result


def summarize_failures(gx_result):
    """Takes GX validator object (the test results) and summarizes any failed tests.
    If there are failures, raises an exception with failed test info.
    If there are no failures, no output is generated.
    """

    failure_count = 0
    failure_summary = {}

    # Print any failed expectations
    for res in gx_result["results"]:
        if not res["success"]:
            failure_count += 1
            failure_info = {}
            failure_info[res["expectation_config"]["expectation_type"]] = res["expectation_config"]["kwargs"]
            failure_summary[failure_count] = failure_info

    failure_string = ""

    for key, value in failure_summary.items():
        failure_string = failure_string + f"{value}\n------\n"

    if failure_count > 0:
        raise Exception(
            f"""The following {failure_count} Great Expectation test(s) failed:

        {failure_string}

        Please review test outputs and correct. Be excellent to each other.
        """
        )
