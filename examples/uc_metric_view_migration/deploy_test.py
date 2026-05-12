#!/usr/bin/env python3
"""
Smoke test UC Metric View YAML against a live Databricks workspace.

Deploys each YAML as a metric view, then runs SELECT MEASURE(name) for each measure.
Reports: which views deploy, which measures execute, which return errors.

Usage:
    python deploy_test.py --catalog my_catalog --schema my_schema \
        --yaml-dir ~/Downloads/ucmv_example_output \
        [--host https://xxx.cloud.databricks.com] \
        [--token dapi...] \
        [--warehouse-id abc123]
        [--dry-run]  # Parse and validate only, don't deploy

Environment variables (alternative to flags):
    DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID
"""
import argparse
import glob
import os
import sys
import urllib.parse

import yaml


def parse_yaml_measures(yaml_path):
    """Extract measure names from a UCMV YAML file."""
    with open(yaml_path) as f:
        data = yaml.safe_load(f)
    if not data or 'measures' not in data:
        return [], data
    measures = []
    for m in data.get('measures', []):
        name = m.get('name', '')
        if name:
            measures.append(name)
    return measures, data


def deploy_view(client, catalog, schema, view_name, yaml_content, warehouse_id):
    """Deploy a metric view using the Databricks SDK."""
    full_name = f"{catalog}.{schema}.{view_name}"
    try:
        response = client.api_client.do(
            'POST',
            '/api/2.0/unity-catalog/metric-views',
            body={
                'name': full_name,
                'yaml_body': yaml_content,
            },
        )
        return {'status': 'deployed', 'view_name': full_name, 'response': response}
    except Exception as e:
        error_msg = str(e)
        # Try update if already exists (409 / ALREADY_EXISTS)
        if '409' in error_msg or 'ALREADY_EXISTS' in error_msg:
            try:
                encoded_name = urllib.parse.quote(full_name, safe='')
                response = client.api_client.do(
                    'PUT',
                    f'/api/2.0/unity-catalog/metric-views/{encoded_name}',
                    body={'yaml_body': yaml_content},
                )
                return {'status': 'updated', 'view_name': full_name, 'response': response}
            except Exception as e2:
                return {'status': 'error', 'view_name': full_name, 'error': str(e2)}
        return {'status': 'error', 'view_name': full_name, 'error': error_msg}


def test_measure(client, catalog, schema, view_name, measure_name, warehouse_id):
    """Execute SELECT MEASURE(name) FROM METRIC VIEW and check result."""
    from databricks.sdk.service.sql import StatementState

    full_name = f"{catalog}.{schema}.{view_name}"
    sql = f"SELECT MEASURE(`{measure_name}`) FROM METRIC VIEW `{catalog}`.`{schema}`.`{view_name}` LIMIT 1"
    try:
        stmt = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout='30s',
        )
        if stmt.status and stmt.status.state == StatementState.SUCCEEDED:
            if stmt.result and stmt.result.data_array:
                value = stmt.result.data_array[0][0] if stmt.result.data_array[0] else None
                if value is None:
                    return {'status': 'null', 'measure': measure_name, 'view': full_name}
                return {'status': 'ok', 'measure': measure_name, 'view': full_name, 'value': str(value)[:50]}
            return {'status': 'ok', 'measure': measure_name, 'view': full_name, 'value': 'empty result'}
        else:
            error = stmt.status.error.message if stmt.status and stmt.status.error else 'Unknown error'
            return {'status': 'error', 'measure': measure_name, 'view': full_name, 'error': error[:200]}
    except Exception as e:
        return {'status': 'error', 'measure': measure_name, 'view': full_name, 'error': str(e)[:200]}


def main():
    parser = argparse.ArgumentParser(description='Smoke test UCMV YAML against Databricks')
    parser.add_argument('--catalog', required=True, help='Target UC catalog')
    parser.add_argument('--schema', required=True, help='Target UC schema')
    parser.add_argument('--yaml-dir', default=os.path.expanduser('~/Downloads/ucmv_example_output'),
                        help='Directory with YAML files (default: ~/Downloads/ucmv_example_output)')
    parser.add_argument('--host', default=os.environ.get('DATABRICKS_HOST', ''),
                        help='Databricks workspace URL (or DATABRICKS_HOST env var)')
    parser.add_argument('--token', default=os.environ.get('DATABRICKS_TOKEN', ''),
                        help='Databricks PAT (or DATABRICKS_TOKEN env var)')
    parser.add_argument('--warehouse-id', default=os.environ.get('DATABRICKS_WAREHOUSE_ID', ''),
                        help='SQL warehouse ID (or DATABRICKS_WAREHOUSE_ID env var)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Parse and validate only, do not deploy')
    parser.add_argument('--skip-deploy', action='store_true',
                        help='Skip deployment, only test existing views')
    args = parser.parse_args()

    # Find YAML files
    yaml_files = sorted(glob.glob(os.path.join(args.yaml_dir, '*_uc_metric_view.yml')))
    if not yaml_files:
        print(f'No YAML files found in {args.yaml_dir}')
        sys.exit(1)

    print(f'{"=" * 60}')
    print('UC METRIC VIEW DEPLOY TEST')
    print(f'{"=" * 60}')
    print(f'YAML dir:    {args.yaml_dir}')
    print(f'Files found: {len(yaml_files)}')
    print(f'Target:      {args.catalog}.{args.schema}')
    print(f'Mode:        {"DRY RUN" if args.dry_run else "LIVE DEPLOY"}')
    print()

    # Phase 1: Parse all YAMLs
    print('Phase 1: Parsing YAML files...')
    views = {}
    total_measures = 0
    parse_errors = 0
    for yf in yaml_files:
        base = os.path.basename(yf).replace('_uc_metric_view.yml', '')
        view_name = f'mv_{base.lower()}'
        try:
            measure_names, yaml_data = parse_yaml_measures(yf)
            with open(yf) as f:
                yaml_content = f.read()
            views[base] = {
                'view_name': view_name,
                'measures': measure_names,
                'yaml_content': yaml_content,
                'yaml_data': yaml_data,
                'path': yf,
            }
            total_measures += len(measure_names)
            print(f'  {base}: {len(measure_names)} measures')
        except Exception as e:
            parse_errors += 1
            print(f'  {base}: PARSE ERROR -- {e}')

    print(f'\nParsed: {len(views)} views, {total_measures} measures, {parse_errors} errors')

    if args.dry_run:
        print('\n[DRY RUN] Skipping deployment and measure testing.')
        # Validate YAML structure
        print('\nPhase 2: Validating YAML structure...')
        valid = 0
        for base, info in views.items():
            data = info['yaml_data']
            issues = []
            if not data:
                issues.append('empty YAML')
            else:
                if not data.get('version'):
                    issues.append('missing version')
                if not data.get('source') and not data.get('measures'):
                    issues.append('missing source and measures')
                for d in data.get('dimensions', []):
                    if not d.get('name') or not d.get('expr'):
                        issues.append(f'empty dimension: {d}')
                        break
            if issues:
                print(f'  {base}: ISSUES -- {", ".join(issues)}')
            else:
                valid += 1
                print(f'  {base}: OK')
        print(f'\nValid: {valid}/{len(views)}')
        return

    # Phase 2: Deploy
    if not args.host or not args.token:
        print('\nERROR: --host and --token required for live deploy '
              '(or set DATABRICKS_HOST, DATABRICKS_TOKEN)')
        sys.exit(1)
    if not args.warehouse_id:
        print('\nERROR: --warehouse-id required for measure testing '
              '(or set DATABRICKS_WAREHOUSE_ID)')
        sys.exit(1)

    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient(host=args.host, token=args.token)

    if not args.skip_deploy:
        print('\nPhase 2: Deploying metric views...')
        deployed = 0
        deploy_errors = 0
        for base, info in views.items():
            result = deploy_view(
                client, args.catalog, args.schema,
                info['view_name'], info['yaml_content'], args.warehouse_id,
            )
            if result['status'] in ('deployed', 'updated'):
                deployed += 1
                print(f'  {base}: {result["status"]}')
            else:
                deploy_errors += 1
                print(f'  {base}: ERROR -- {result["error"][:100]}')
        print(f'\nDeployed: {deployed}, Errors: {deploy_errors}')

    # Phase 3: Test measures
    print('\nPhase 3: Testing measures...')
    ok_count = 0
    null_count = 0
    error_count = 0
    for base, info in views.items():
        for measure_name in info['measures']:
            result = test_measure(
                client, args.catalog, args.schema,
                info['view_name'], measure_name, args.warehouse_id,
            )
            if result['status'] == 'ok':
                ok_count += 1
            elif result['status'] == 'null':
                null_count += 1
                print(f'  {base}/{measure_name}: NULL result')
            else:
                error_count += 1
                print(f'  {base}/{measure_name}: ERROR -- {result["error"][:100]}')

    # Summary
    print(f'\n{"=" * 60}')
    print('RESULTS')
    print(f'{"=" * 60}')
    print(f'Views:    {len(views)} total')
    print(f'Measures: {total_measures} total')
    print(f'  OK:     {ok_count}')
    print(f'  NULL:   {null_count}')
    print(f'  ERROR:  {error_count}')
    print(f'Success rate: {ok_count * 100 // total_measures if total_measures else 0}%')


if __name__ == '__main__':
    main()
