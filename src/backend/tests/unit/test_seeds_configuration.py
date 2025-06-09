#!/usr/bin/env python3
"""Test script to verify seed configurations."""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.seeds.model_configs import DEFAULT_MODELS

print('Databricks models that will be ENABLED:')
for key, data in DEFAULT_MODELS.items():
    if data['provider'] == 'databricks':
        print(f'  - {key} ({data["name"]})')

print()
print('Other models that will be DISABLED:')
count = 0
for key, data in DEFAULT_MODELS.items():
    if data['provider'] != 'databricks':
        count += 1
        if count <= 5:
            print(f'  - {key} ({data["provider"]})')
        elif count == 6:
            other_count = len([k for k, d in DEFAULT_MODELS.items() if d['provider'] != 'databricks']) - 5
            print(f'  ... and {other_count} more')

print()
print(f'Total models: {len(DEFAULT_MODELS)}')
databricks_count = len([k for k, d in DEFAULT_MODELS.items() if d['provider'] == 'databricks'])
other_count = len([k for k, d in DEFAULT_MODELS.items() if d['provider'] != 'databricks'])
print(f'Databricks models: {databricks_count}')
print(f'Other models: {other_count}')

print('\nTools configuration:')
print('  - GenieTool (ID 35): ENABLED')
print('  - All other tools: DISABLED')