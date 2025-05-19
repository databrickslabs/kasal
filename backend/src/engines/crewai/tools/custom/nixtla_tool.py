from typing import Any, Dict, List
from langchain.tools import BaseTool
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

# Try to import TimeGPT, but provide a mock if it's not available
try:
    from nixtla.timegpt import TimeGPT
    TIMEGPT_AVAILABLE = True
except ImportError:
    try:
        from timegpt import TimeGPT
        TIMEGPT_AVAILABLE = True
    except ImportError:
        TIMEGPT_AVAILABLE = False
        logger.warning("TimeGPT not available. Using mock implementation.")

        class TimeGPT:
            def predict(self, df, h, freq, level):
                return pd.DataFrame({
                    'ds': pd.date_range(start='2024-01-01', periods=h, freq=freq),
                    'y': [100 + i * 10 for i in range(h)]
                })

class NixtlaTimeGPTTool(BaseTool):
    name: str = "nixtla_timegpt"
    description: str = """
    Use this tool for time series forecasting using Nixtla's TimeGPT.
    Input should be a dictionary with the following keys:
    - data: List of dictionaries containing 'ds' (datetime) and 'y' (value) pairs
    - horizon: Number of periods to forecast
    - frequency: Data frequency (e.g., 'D' for daily, 'M' for monthly)
    - confidence_levels: Optional list of confidence intervals (e.g., [80, 95])
    """

    def __init__(self, api_key: str):
        super().__init__()
        self.api_key = api_key
        if TIMEGPT_AVAILABLE:
            os.environ['TIMEGPT_API_KEY'] = api_key
        self.model = TimeGPT()

    def _run(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        try:
            # Convert input data to DataFrame
            df = pd.DataFrame(tool_input['data'])
            df['unique_id'] = 'series_1'
            df['ds'] = pd.to_datetime(df['ds'])

            # Generate forecasts
            forecasts_df = self.model.predict(
                df=df,
                h=tool_input['horizon'],
                freq=tool_input['frequency'],
                level=tool_input.get('confidence_levels', [80, 95])
            )

            # Convert forecast results to dictionary
            return {
                'forecasts': forecasts_df.to_dict(orient='records')
            }

        except Exception as e:
            logger.error(f"Error in NixtlaTimeGPTTool: {str(e)}")
            return {'error': str(e)}

    async def _arun(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        return self._run(tool_input) 