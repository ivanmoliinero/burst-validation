import logging
from math import floor
import random
import json
import argparse
import boto3
import pandas as pd
import numpy as np
from io import BytesIO


logger = logging.getLogger(__name__)


DEFAULT_MAX_SAMPLE_SIZE: int = 1 * 1024 * 1024
DEFAULT_SAMPLE_RATIO: float = 0.01
DEFAULT_SAMPLE_FRAGMENTS: int = 20
DEFAULT_START_MARGIN: float = 0.02
DEFAULT_END_MARGIN: float = 0.02
DEFUALT_BOUND_EXTRACTION_MARGIN: int = 1024 * 1024
DEFAULT_PAYLOAD_FILENAME = "sort_payload"
DEFAULT_TMP_PREFIX = "tmp/"

AWS_S3_REGION = None
AWS_S3_ENDPOINT = None
AWS_ACCESS_KEY_ID = None
AWS_SECRET_ACCESS_KEY = None


def main():
    parser = argparse.ArgumentParser(description="Generate Sort input payload")
    parser.add_argument(
        "--partitions", type=int, required=True, help="Number of partitions"
    )
    parser.add_argument("--bucket", type=str, required=True, help="Bucket name")
    parser.add_argument("--key", type=str, required=True, help="Object key")
    parser.add_argument(
        "--sort-output-key", type=str, required=False, help="Sort output key"
    )
    parser.add_argument("--sort-column", type=int, required=True, help="Sort key")
    parser.add_argument("--delimiter", type=str, default=",", help="Delimiter")
    parser.add_argument(
        "--start-margin", type=float, default=DEFAULT_START_MARGIN, help="Start margin"
    )
    parser.add_argument(
        "--end-margin", type=float, default=DEFAULT_END_MARGIN, help="End margin"
    )
    parser.add_argument(
        "--sample-ratio", type=float, default=DEFAULT_SAMPLE_RATIO, help="Sample ratio"
    )
    parser.add_argument(
        "--sample-fragments",
        type=int,
        default=DEFAULT_SAMPLE_FRAGMENTS,
        help="Sample fragments",
    )
    parser.add_argument(
        "--max-sample-size",
        type=int,
        default=DEFAULT_MAX_SAMPLE_SIZE,
        help="Max sample size",
    )
    parser.add_argument(
        "--bound-margin",
        type=int,
        default=DEFUALT_BOUND_EXTRACTION_MARGIN,
        help="Bound extraction margin",
    )
    parser.add_argument("--seed", type=int, default=None, help="Random seed")
    parser.add_argument(
        "--payload-filename",
        type=str,
        default=DEFAULT_PAYLOAD_FILENAME,
        help="Payload filename",
    )
    parser.add_argument(
        "--tmp-prefix",
        type=str,
        default=DEFAULT_TMP_PREFIX,
        help="Prefix for temorary data in S3",
    )
    parser.add_argument(
        "--s3_region", type=str, default=AWS_S3_REGION, help="S3 region"
    )
    parser.add_argument(
        "--s3_endpoint", type=str, default=AWS_S3_ENDPOINT, help="S3 endpoint"
    )
    parser.add_argument(
        "--aws_access_key_id",
        type=str,
        default=AWS_ACCESS_KEY_ID,
        help="AWS access key id",
    )
    parser.add_argument(
        "--aws_secret_access_key",
        type=str,
        default=AWS_SECRET_ACCESS_KEY,
        help="AWS secret access key",
    )
    parser.add_argument(
        "--split", type=int, default=1, help="Split output into multiple files"
    )
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    # Build S3 client kwargs - only include credentials if explicitly provided
    s3_kwargs: dict = {
        "endpoint_url": args.s3_endpoint,
        "region_name": args.s3_region,
    }
    if args.aws_access_key_id and args.aws_secret_access_key:
        s3_kwargs["aws_access_key_id"] = args.aws_access_key_id
        s3_kwargs["aws_secret_access_key"] = args.aws_secret_access_key

    s3_client = boto3.client("s3", **s3_kwargs)
    obj_size = s3_client.head_object(Bucket=args.bucket, Key=args.key)["ContentLength"]

    # Avoid dataset head and tail
    start_limit = int(obj_size * DEFAULT_START_MARGIN)
    end_limit = int(obj_size * (1 - DEFAULT_END_MARGIN))
    choosable_size = end_limit - start_limit

    # Size of each sampled fragment
    fragment_size = floor(
        min(
            floor((end_limit - start_limit) * DEFAULT_SAMPLE_RATIO),
            DEFAULT_MAX_SAMPLE_SIZE,
        )
        / DEFAULT_SAMPLE_FRAGMENTS
    )

    # Select bounds randomly
    num_parts = int(choosable_size / fragment_size)
    selected_fragments = sorted(
        random.sample(range(num_parts), DEFAULT_SAMPLE_FRAGMENTS)
    )

    keys_arrays = []
    row_lens = []

    # Read from each bound a fragment size, adjusting limits
    for f in selected_fragments:
        lower_bound = start_limit + f * fragment_size
        upper_bound = lower_bound + fragment_size

        range_0 = max(0, lower_bound - args.bound_margin)
        range_1 = min(obj_size, upper_bound + args.bound_margin)

        body = s3_client.get_object(
            Bucket=args.bucket,
            Key=args.key,
            Range=f"bytes={range_0}-{range_1}",
        )["Body"].read()

        body_sz = len(body)
        start_byte = lower_bound - range_0
        end_byte = upper_bound - range_1
        if start_byte > 0:
            lower_bound = start_byte

            while lower_bound > 0:
                if body[lower_bound : lower_bound + 1] == b"\n":
                    lower_bound += 1
                    break
                else:
                    lower_bound -= 1
        else:
            lower_bound = 0

        if end_byte < body_sz:
            upper_bound = end_byte

            while upper_bound < body_sz:
                if body[upper_bound : upper_bound + 1] == b"\n":
                    break
                else:
                    upper_bound += 1
        else:
            upper_bound = end_byte

        body_memview = memoryview(body)
        partition = body_memview[lower_bound:upper_bound]

        # find index of \n from the beginning of the body
        buff = BytesIO(partition)
        row_len = len(buff.readline())
        buff.seek(0)
        row_lens.append(row_len)

        df = pd.read_csv(
            BytesIO(partition),
            engine="c",
            index_col=None,
            header=None,
            delimiter=args.delimiter,
            quoting=3,
            on_bad_lines="warn",
        )

        keys_arrays.append(np.array(df[args.sort_column]))

    # Assert all row lengths are the same
    assert len(set(row_lens)) == 1
    row_len = set(row_lens).pop()

    # Concat keys, sort them
    keys = np.concatenate(keys_arrays)
    keys.sort()

    # Find quantiles (num tasks)
    quantiles = [i * 1 / args.partitions for i in range(1, args.partitions)]
    segment_bounds = [keys[int(q * len(keys))] for q in quantiles]

    # Generate multipart upload
    output_key = (
        args.sort_output_key
        if args.sort_output_key is not None
        else args.key + ".sorted"
    )
    mpu_res = s3_client.create_multipart_upload(Bucket=args.bucket, Key=output_key)
    # print(mpu_res)
    mpu_id = mpu_res["UploadId"]

    # pprint(segment_bounds)

    # Write parameters as JSON file
    params = [
        {
            "bucket": args.bucket,
            "key": args.key,
            "obj_size": obj_size,
            "sort_column": args.sort_column,
            "delimiter": args.delimiter,
            "partitions": args.partitions,
            "partition_idx": i,
            "segment_bounds": segment_bounds,
            "row_size": row_len,
            "mpu_key": output_key,
            "mpu_id": mpu_id,
            "tmp_prefix": args.tmp_prefix,
            "s3_config": {
                "region": args.s3_region,
                "endpoint": args.s3_endpoint,
                "aws_access_key_id": args.aws_access_key_id,
                "aws_secret_access_key": args.aws_secret_access_key,
            },
        }
        for i in range(args.partitions)
    ]

    if args.split > 1:
        assert args.partitions % args.split == 0
        stride = args.partitions // args.split
        for i in range(args.split):
            with open(
                args.payload_filename + f"_part-{str(i).zfill(4)}" + ".json", "w"
            ) as f:
                f.write(
                    json.dumps(params[stride * i : (stride * i) + stride], indent=4)
                )
    else:
        with open(f"{args.payload_filename}.json", "w") as f:
            json.dump(params, f, indent=4)


if __name__ == "__main__":
    main()
