
class PipelineOptions:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def start_dlp_inspection_pipeline(options):

    options_str = f'--project={options.projectId} \
    --region={options.regionId} \
    --runner=DataflowRunner \
    --serviceAccount={options.serviceAccount} \
    --gcpTempLocation=gs://{options.tempGcsBucket}/temp \
    --stagingLocation=gs://{options.tempGcsBucket}/staging \
    --tempLocation=gs://{options.tempGcsBucket}/bqtemp \
    --workerMachineType=n1-standard-1 \
    --subnetwork=https://www.googleapis.com/compute/v1/projects/{options.projectId}/regions/{options.regionId}/subnetworks/{options.subnetworkName} \
    --sampleSize=600 \
    --sourceType=PARQUET \
    --inputPattern={options.inputPattern} \
    --reportLocation={options.reportLocation}'

    _run_pipeline('com.google.cloud.solutions.autotokenize.pipeline.DlpInspectionPipeline', options_str.split(' '))


def start_tokenize_pipeline(options):
    options_str = f'--project={options.projectId} \
    --region={options.regionId} \
    --runner=DataflowRunner \
    --serviceAccount={options.serviceAccount} \
    --tempLocation=gs://{options.tempGcsBucket}/bqtemp \
    --workerMachineType="n1-standard-1" \
    --schema="{options.schemaLocation}" \
    --mainKmsKeyUri="{options.mainKmsKeyUri}" \
    --keyMaterialType="{options.keyMaterialType}" \
    --keyMaterial="{options.keyMaterial}" \
    --subnetwork=https://www.googleapis.com/compute/v1/projects/{options.projectId}/regions/{options.regionId}/subnetworks/{options.subnetworkName} \
    --sourceType="PARQUET" \
    --inputPattern={options.inputPattern} \
    --outputDirectory={options.outputDirectory} \
    --tokenizeColumns={" ".join(map(lambda col: "--tokenizeColumns " + col, options.tokenize_columns))}'

    _run_pipeline('com.google.cloud.solutions.autotokenize.pipeline.EncryptionPipeline', options_str.split(' '))


def _run_pipeline(pipeline_name, options):
    from subprocess import Popen, PIPE, STDOUT
    import os
    p = Popen(['java', '-cp', os.environ['AUTO_TOKENIZE_JAR'], pipeline_name] + options, stdout=PIPE, stderr=STDOUT)

    while True:
        # Wait for some output, read it and print it.
        output = p.stdout.read1(1024).decode('utf-8')
        print(output, end='')

        # Has the subprocess finished yet?
        if p.poll() is not None:
            break

    if p.returncode != 0:
        print("Exited with error code:", p.returncode)