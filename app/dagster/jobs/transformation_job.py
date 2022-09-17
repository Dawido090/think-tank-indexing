from dagster import get_dagster_logger, job, op, mem_io_manager, in_process_executor
from minio import Minio, error
import io

def get_minio_client():
    Client = Minio(
        'minio-raw:9000',
        access_key='minioadmin',
        secret_key='minioadmin',
        secure = False
    )
    return Client

def check_minio_connection(client):
    try:
        if not client.bucket_exists("nonexistingbucket"):
            get_dagster_logger().info("Object storage connected")
            return True
    except:
        # raise error if storage not reachable
        get_dagster_logger().error("Object storage not reachable")
        return False

@op
def check_what_is_transformad():
    client = get_minio_client()
    if check_minio_connection(client) == False:
        get_dagster_logger().error('Minio is down!')
    transformed = [x.object_name.split('/')[1] for x in client.list_objects('articles',prefix='transformed/')]
    if len(transformed) == 0:
        get_dagster_logger().info('No files in transformed')
        return None
    else:
        return transformed

@op
def get_raw_data_files_names(transformed):
    client = get_minio_client()
    if check_minio_connection(client) == False:
        get_dagster_logger().error('Minio is down!')

    raw_data = [x.object_name.split('/')[1] for x in client.list_objects('articles',prefix='raw-data/')]
    if transformed != None:
        checked_raw_data = [x for x in raw_data if x not in transformed]
        return checked_raw_data
    if transformed == None:
        return raw_data 



@op
def move_raw_data_to_transformed(files_which_can_be_moved):
    client = get_minio_client()
    if check_minio_connection(client) == False:
        get_dagster_logger().error('Minio is down!')

    raw_data_to_be_moved ={}

    for file in files_which_can_be_moved:
        data = client.get_object('articles',object_name= 'raw-data/' + file).data.decode().encode()
        raw_data_to_be_moved[file] = data

    count_of_files_added = 0
    for object in raw_data_to_be_moved.items():
        client.put_object('articles','transformed/' + object[0] ,data = io.BytesIO(object[1]),length=len(object[1]))
        count_of_files_added += 1
    get_dagster_logger().info(f'{count_of_files_added} files added to transformed')
    #     client.fput_object('articles',object_name = 'raw-data/' + file,file_path=file)
    #     count_of_files_added += 1
    # get_dagster_logger().info(f'{count_of_files_added} files added to transformed')
    # for file in os.listdir():
    #     os.remove(file)

@job(resource_defs={"io_manager": mem_io_manager}, executor_def=in_process_executor)
def transform_raw_data_job():
    already_transformed = check_what_is_transformad()
    get_raw_data = get_raw_data_files_names(already_transformed)
    move_raw_data = move_raw_data_to_transformed(get_raw_data)

# take a look at files inside transformed folder, return list or empty if folder doesn't exist
# take data from raw-data minio
# take only files which are not in transformed
# apply pseudo transformation, to be filled later

