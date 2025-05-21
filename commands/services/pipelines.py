import boto3


class CodePipelineService:
    def get_pipeline_details(self, profile):
        session = (
            boto3.Session(profile_name=profile)
            if profile
            else boto3.Session()
        )
        codepipeline = session.client("codepipeline")

        pipelines = codepipeline.list_pipelines()["pipelines"]
        results = []

        for pipeline in pipelines:
            pipeline_name = pipeline["name"]
            response = codepipeline.get_pipeline_state(name=pipeline_name)

            git_branch = None
            repo_name = None
            latest_status = None

            for stage_state in response["stageStates"]:
                if stage_state["stageName"] == "Source":
                    for action_state in stage_state["actionStates"]:
                        entity_url = action_state.get("entityUrl", "")
                        if "Branch=" in entity_url:
                            git_branch = entity_url.split("Branch=")[-1].split("&")[0]
                        if "FullRepositoryId=" in entity_url:
                            repo_name = (
                                entity_url.split("FullRepositoryId=")[-1].split("&")[0]
                            )
                        latest_status = stage_state.get("latestExecution", {}).get(
                            "status", "Unknown"
                        )
                        break

            results.append(
                {
                    "pipeline": pipeline_name,
                    "repository": repo_name or "Unknown",
                    "branch": git_branch or "Unknown",
                    "status": latest_status,
                }
            )

        return results