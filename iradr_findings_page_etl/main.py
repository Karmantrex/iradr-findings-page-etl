from telescope.log.base import get_logger

from utils.schedule.runner import JobRunner

from settings.job import (
    job_settings,
    jobstore_settings,
    scheduler_settings,
    runner_settings
)
from settings.job import job_types

logger = get_logger()


if __name__ == '__main__':

    pipeline = job_types[job_settings.name](
        **job_settings.dict()
    )
    
    runner = JobRunner(
        job_id=pipeline.name,
        func=pipeline.etl,
        jobstore_kwargs=jobstore_settings.dict(),
        scheduler_kwargs=scheduler_settings.dict(),
        **runner_settings.dict()
    )

    runner.run()
