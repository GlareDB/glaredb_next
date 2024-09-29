import duckdb
import datafusion
import rayexec
import pathlib
import pandas as pd
import time
import os

# TPC-H scale factor.
sf = 10


def generate_data():
    con = duckdb.connect()
    con.sql("PRAGMA disable_progress_bar;SET preserve_insertion_order=false")
    con.sql(f"CALL dbgen(sf={sf})")
    if os.path.isdir(f"./benchmarks/data/tpch-{sf}"):
        return
    pathlib.Path(f"./benchmarks/data/tpch-{sf}").mkdir(parents=True, exist_ok=True)
    for tbl in [
        "nation",
        "region",
        "customer",
        "supplier",
        "lineitem",
        "orders",
        "partsupp",
        "part",
    ]:
        con.sql(
            f"COPY (SELECT * FROM {tbl}) TO './benchmarks/data/tpch-{sf}/{tbl}.parquet'"
        )
    con.close()


generate_data()

queries = {
    1: """
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= date '1998-09-02'
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;
    """,
    2: """
SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM
      part,
      supplier,
      partsupp,
      nation,
      region
WHERE
    p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = 15
    AND p_type LIKE '%BRASS'
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND ps_supplycost = (
        SELECT
            min(ps_supplycost)
        FROM
            partsupp,
            supplier,
            nation,
            region
        WHERE
            p_partkey = ps_partkey
            AND s_suppkey = ps_suppkey
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_name = 'EUROPE')
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
LIMIT 100;
    """,
    3: """
SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('1995-03-15' AS date)
    AND l_shipdate > CAST('1995-03-15' AS date)
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10;
    """,
}


def setup_rayexec(conn):
    for tbl in [
        "nation",
        "region",
        "customer",
        "supplier",
        "lineitem",
        "orders",
        "partsupp",
        "part",
    ]:
        conn.query(
            f"CREATE TEMP VIEW {tbl} AS SELECT * FROM './benchmarks/data/tpch-{sf}/{tbl}.parquet'"
        )


def setup_datafusion(ctx):
    for tbl in [
        "nation",
        "region",
        "customer",
        "supplier",
        "lineitem",
        "orders",
        "partsupp",
        "part",
    ]:
        ctx.register_parquet(tbl, f"./benchmarks/data/tpch-{sf}/{tbl}.parquet")


def execute_rayexec(conn, dump_profile=False):
    df = pd.DataFrame(columns=["dur", "query"])
    for query_id, query in sorted(queries.items()):
        start = time.time()
        print("Query " + str(query_id))
        try:
            collect_profile_data = dump_profile
            table = conn.query(query, collect_profile_data)
            table.show()
            stop = time.time()
            duration = stop - start
            if dump_profile:
                table.dump_profile()
        except Exception as err:
            print(err)
            duration = 0
        print(duration)
        row = {"dur": duration, "query": query_id}
        df = pd.concat(
            [
                df if not df.empty else None,
                pd.DataFrame(row, index=[query_id]),
            ],
            axis=0,
            ignore_index=True,
        )
    return df


def execute_datafusion(ctx):
    df = pd.DataFrame(columns=["dur", "query"])
    for query_id, query in sorted(queries.items()):
        start = time.time()
        print("Query " + str(query_id))
        try:
            print(ctx.sql(query))
            stop = time.time()
            duration = stop - start
        except Exception as er:
            print(er)
            duration = 0
        print(duration)
        row = {"dur": duration, "query": query_id}
        df = pd.concat(
            [
                df if not df.empty else None,
                pd.DataFrame(row, index=[query_id]),
            ],
            axis=0,
            ignore_index=True,
        )
    return df


rayexec_conn = rayexec.connect()
setup_rayexec(rayexec_conn)

datafusion_ctx = datafusion.SessionContext()
setup_datafusion(datafusion_ctx)

rayexec_times = execute_rayexec(rayexec_conn, False)
rayexec_conn.close()

datafusion_times = execute_datafusion(datafusion_ctx)

print("RAYEXEC")
print(rayexec_times)
print("DATAFUSION")
print(datafusion_times)
