import json
import boto3
from datetime import datetime, timedelta, timezone

def lambda_handler(event, context):
    glue = boto3.client('glue')
    emr = boto3.client('emr')
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)

    glue_runs = []
    emr_clusters = []
    named_clusters = {
        "your-own-cluster-name": None,
        "your-own-cluster-name": None,
        "your-own-cluster-name": None,
    }

    try:

        jobs_response = glue.get_jobs()
        job_list = jobs_response['Jobs']
        print(f"Found {len(job_list)} Glue jobs")

        for job in job_list:
            job_name = job['Name']
            runs_response = glue.get_job_runs(JobName=job_name)
            job_runs = runs_response.get('JobRuns', [])

            for run in job_runs:
                started_on = run.get('StartedOn')
                if started_on and started_on > cutoff:
                    run_id = run.get('Id')
                    print(f"Getting detailed info for Glue run: {job_name} / {run_id}")
                    detailed = glue.get_job_run(JobName=job_name, RunId=run_id)['JobRun']

                    glue_runs.append({
                        'jobName': job_name,
                        'runId': run_id,
                        'status': detailed.get('JobRunState'),
                        'startedOn': detailed.get('StartedOn').isoformat(),
                        'completedOn': detailed.get('CompletedOn').isoformat() if detailed.get('CompletedOn') else None,
                        'executionTime': detailed.get('ExecutionTime'),
                        'arguments': detailed.get('Arguments'),
                        'errorMessage': detailed.get('ErrorMessage'),
                        'glueVersion': detailed.get('GlueVersion'),
                        'workerType': detailed.get('WorkerType'),
                        'numberOfWorkers': detailed.get('NumberOfWorkers'),
                        'timeout': detailed.get('Timeout')
                    })

        print(f"Total Glue job runs in last 24 hours: {len(glue_runs)}")

        
        clusters_response = emr.list_clusters(ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'])
        clusters = clusters_response.get('Clusters', [])

        for cluster in clusters:
            cluster_id = cluster['Id']
            summary = emr.describe_cluster(ClusterId=cluster_id)['Cluster']
            name = summary.get('Name', 'Unknown')
            timeline = summary.get('Status', {}).get('Timeline', {})
            apps = [app['Name'] for app in summary.get('Applications', [])]

            cluster_data = {
                'clusterId': cluster_id,
                'clusterName': name,
                'status': summary.get('Status', {}).get('State'),
                'createdOn': timeline.get('CreationDateTime').isoformat() if timeline.get('CreationDateTime') else None,
                'normalizedInstanceHours': summary.get('NormalizedInstanceHours'),
                'releaseLabel': summary.get('ReleaseLabel'),
                'instanceType': summary.get('InstanceCollectionType', 'UNKNOWN'),
                'logUri': summary.get('LogUri'),
                'masterPublicDnsName': summary.get('MasterPublicDnsName'),
                'instanceCount': summary.get('InstanceFleetType', 'N/A'),
                'stepConcurrencyLevel': summary.get('StepConcurrencyLevel'),
                'terminationProtected': summary.get('TerminationProtected'),
                'securityConfiguration': summary.get('SecurityConfiguration', 'None'),
                'applications': apps,
                'tags': {tag['Key']: tag['Value'] for tag in summary.get('Tags', [])}
            }

            emr_clusters.append(cluster_data)

            if name in named_clusters:
                named_clusters[name] = cluster_data

        print(f"Total EMR clusters enriched: {len(emr_clusters)}")

        # FINAL PAYLOAD
        result = {
            'glueJobRunData': glue_runs,
            'emrClusterData': emr_clusters
        }

        for name, data in named_clusters.items():
            result[name] = data

        return {
            'statusCode': 200,
            'body': json.dumps(result, default=str)
        }

    except Exception as e:
        print(f"🔥 Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
