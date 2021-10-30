#!/usr/bin/env python
"""
 Download from W&B the raw dataset and apply some basic cleaning, exporting the results to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    local_path = wandb.use_artifact(args.input_artifact).file()
    df = pd.read_csv(local_path)

    logger.info("Dropping outliers")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    logger.info("Converting last_review to date format")
    df['last_review'] = pd.to_datetime(df['last_review'])

    logger.info("Save cleaned dataset")
    filename = args.output_artifact
    df.to_csv(filename, index=False)

    logger.info("load cleaned data to W&B")
    artifact = wandb.Artifact(
            args.output_artifact,
            type=args.output_type,
            description=args.output_description,
    )
    artifact.add_file(args.output_artifact)
    run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type= str,
        help="This is the raw initial data",
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="This is the cleaned dataset",
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="This is a cleaned csv file",
        required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="cleaned dataset",
        required=True
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="Minimum price accepted after discussion with stakeholders",
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="This is the max price accepted after discussion with stakeholders",
        required=True
    )


    args = parser.parse_args()

    go(args)
