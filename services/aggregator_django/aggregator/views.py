# services/aggregator_django/aggregator/views.py

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
from .serializers import AggregationRequestSerializer, StockSummaryRequestSerializer, MultiStockSummaryRequestSerializer
import pytz

def get_questdb_connection():
    """
    Establishes a connection to QuestDB.
    """
    return psycopg2.connect(
        dbname=settings.DATABASES['questdb']['NAME'],
        user=settings.DATABASES['questdb']['USER'],
        password=settings.DATABASES['questdb']['PASSWORD'],
        host=settings.DATABASES['questdb']['HOST'],
        port=settings.DATABASES['questdb']['PORT']
    )

def get_average(cursor,stock_symbol, start_time_str, end_time_str, field):
    query = sql.SQL("""
        SELECT AVG({field})
        FROM stock_data
        WHERE stock_symbol = %s
          AND local_time >= %s
          AND local_time <= %s
    """).format(field=sql.Identifier(field))
    cursor.execute(query, [stock_symbol, start_time_str, end_time_str])
    result = cursor.fetchone()
    return result[0]

def get_highest(cursor,stock_symbol, start_time_str, end_time_str, field):
    query = sql.SQL("""
        SELECT MAX({field})
        FROM stock_data
        WHERE stock_symbol = %s
            AND local_time >= %s
            AND local_time <= %s
    """).format(field=sql.Identifier(field))
    cursor.execute(query, [stock_symbol, start_time_str, end_time_str])
    result = cursor.fetchone()
    return result[0]

def get_lowest(cursor,stock_symbol, start_time_str, end_time_str, field):
    query = sql.SQL("""
        SELECT MIN({field})
        FROM stock_data
        WHERE stock_symbol = %s
            AND local_time >= %s
            AND local_time <= %s
    """).format(field=sql.Identifier(field))
    cursor.execute(query, [stock_symbol, start_time_str, end_time_str])
    result = cursor.fetchone()
    return result[0]

def summarize_single_stock(cursor,stock_symbol, start_time_str, end_time_str):
    # Perform a single SQL query to get all required aggregations
    query = sql.SQL("""
        SELECT
            AVG(close) AS avg_close,
            MAX(close) AS highest_close,
            MIN(close) AS lowest_close,
            AVG(SMA_5) AS avg_SMA_5,
            MAX(SMA_5) AS highest_SMA_5,
            MIN(SMA_5) AS lowest_SMA_5,
            AVG(EMA_10) AS avg_EMA_10,
            MAX(EMA_10) AS highest_EMA_10,
            MIN(EMA_10) AS lowest_EMA_10,
            AVG(RSI_10) AS avg_RSI_10,
            MAX(RSI_10) AS highest_RSI_10,
            MIN(RSI_10) AS lowest_RSI_10,
            MAX(gain) AS highest_gain,
            MAX(loss) AS highest_loss
        FROM stock_data
        WHERE stock_symbol = %s
        AND local_time >= %s
        AND local_time <= %s
    """)

    cursor.execute(query, [stock_symbol, start_time_str, end_time_str])
    return cursor.fetchone()

def build_single_summary_response(result):
    return {
        "close": {
            "avg": result[0],
            "highest": result[1],
            "lowest": result[2]
        },
        "SMA_5": {
            "avg": result[3],
            "highest": result[4],
            "lowest": result[5]
        },
        "EMA_10": {
            "avg": result[6],
            "highest": result[7],
            "lowest": result[8]
        },
        "RSI_10": {
            "avg": result[9],
            "highest": result[10],
            "lowest": result[11]
        },
        "gain_loss": {
            "highest_gain": result[12],
            "highest_loss": result[13]
        }
    }


class AggregationView(APIView):
    """
    API View to handle aggregation requests: avg, highest, lowest.
    """

    def post(self, request, format=None):
        serializer = AggregationRequestSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        aggregation = serializer.validated_data['aggregation']
        stock_symbol = serializer.validated_data['stock_symbol']
        period = serializer.validated_data['period']
        field = serializer.validated_data['field']

        # Define the timezone as Asia/Tehran
        tehran_tz = pytz.timezone('Asia/Tehran')

        # Get the current time in Asia/Tehran timezone
        end_time = datetime.now(tehran_tz)

        # Calculate the start time by subtracting the period
        start_time = end_time - timedelta(minutes=period)

        # Convert datetime objects to naive datetime (remove timezone info)
        # This is crucial to prevent QuestDB from interpreting them as timestamptz
        start_time_naive = start_time.replace(tzinfo=None)
        end_time_naive = end_time.replace(tzinfo=None)

        # Format the naive datetime objects as ISO 8601 strings without timezone
        # Example format: '2025-01-23T02:33:22'
        start_time_str = start_time_naive.isoformat(sep=' ')
        end_time_str = end_time_naive.isoformat(sep=' ')

        try:
            conn = get_questdb_connection()
            cursor= conn.cursor()
            if aggregation == 'avg':
                aggregation_result = get_average(cursor,stock_symbol, start_time_str, end_time_str, field)
                response_data = {'avg': aggregation_result, 'field': field, 'stock_symbol': stock_symbol}

            elif aggregation == 'highest':
                aggregation_result = get_highest(cursor,stock_symbol, start_time_str, end_time_str, field)
                response_data = {'highest': aggregation_result, 'field': field, 'stock_symbol': stock_symbol}

            elif aggregation == 'lowest':
                aggregation_result = get_lowest(cursor,stock_symbol, start_time_str, end_time_str, field)
                response_data = {'lowest': aggregation_result, 'field': field, 'stock_symbol': stock_symbol}

            else:
                return Response({'error': 'Invalid aggregation type.'}, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            return Response({'error': 'Database error', 'details': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        return Response(response_data, status=status.HTTP_200_OK)
    
class StockSummaryView(APIView):
    """
    API View to summarize a single stock by calculating highest, lowest,
    average for multiple fields, and highest gain and loss.
    """

    def post(self, request, format=None):
        serializer = StockSummaryRequestSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        stock_symbol = serializer.validated_data['stock_symbol']
        period = serializer.validated_data['period']

        # Define the timezone as Asia/Tehran
        tehran_tz = pytz.timezone('Asia/Tehran')

        # Get the current time in Asia/Tehran timezone
        end_time = datetime.now(tehran_tz)

        # Calculate the start time by subtracting the period
        start_time = end_time - timedelta(minutes=period)

        # Convert datetime objects to naive datetime (remove timezone info)
        # This is crucial to prevent QuestDB from interpreting them as timestamptz
        start_time_naive = start_time.replace(tzinfo=None)
        end_time_naive = end_time.replace(tzinfo=None)

        # Format the naive datetime objects as ISO 8601 strings without timezone
        # Example format: '2025-01-23 02:33:22'
        start_time_str = start_time_naive.strftime('%Y-%m-%d %H:%M:%S')
        end_time_str = end_time_naive.strftime('%Y-%m-%d %H:%M:%S')

        try:
            conn = get_questdb_connection()
            cursor = conn.cursor()

            result = summarize_single_stock(cursor,stock_symbol, start_time_str, end_time_str)

            if result is None:
                return Response({'error': 'No data found for the given parameters.'}, status=status.HTTP_404_NOT_FOUND)

            # Build the response data
            single_stock_summary = build_single_summary_response(result)
            response_data = {
                "stock_symbol": stock_symbol,
                "period": period,
                "summary": single_stock_summary
            }

        except Exception as e:
            return Response({'error': 'Database error', 'details': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        return Response(response_data, status=status.HTTP_200_OK)
    
class MultiStockSummaryView(APIView):
    """
    API View to summarize multiple stocks by calculating highest, lowest,
    average for multiple fields, and highest gain and loss for each stock.
    """

    def post(self, request, format=None):
        serializer = MultiStockSummaryRequestSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        stock_symbols = serializer.validated_data['stock_symbol']
        period = serializer.validated_data['period']

        # Define the timezone as Asia/Tehran
        tehran_tz = pytz.timezone('Asia/Tehran')

        # Get the current time in Asia/Tehran timezone
        end_time = datetime.now(tehran_tz)

        # Calculate the start time by subtracting the period
        start_time = end_time - timedelta(minutes=period)

        # Convert datetime objects to naive datetime (remove timezone info)
        # This is crucial to prevent QuestDB from interpreting them as timestamptz
        start_time_naive = start_time.replace(tzinfo=None)
        end_time_naive = end_time.replace(tzinfo=None)

        # Format the naive datetime objects as ISO 8601 strings without timezone
        # Example format: '2025-01-23 02:33:22'
        start_time_str = start_time_naive.strftime('%Y-%m-%d %H:%M:%S')
        end_time_str = end_time_naive.strftime('%Y-%m-%d %H:%M:%S')

        summaries = {}
        errors = {}

        try:
            conn = get_questdb_connection()
            if not conn:
                raise Exception("Failed to establish database connection.")
            cursor = conn.cursor()

            for symbol in stock_symbols:
                try:
                    result = summarize_single_stock(cursor, symbol, start_time_str, end_time_str)
                    if result:
                        summaries[symbol] = build_single_summary_response(result)
                    else:
                        errors[symbol] = "No data found for this stock and period."
                except Exception as e:
                    errors[symbol] = str(e)

        except Exception as e:
            return Response({'error': 'Database connection error', 'details': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        response_data = {
            "period": period,
            "summaries": summaries,
            "errors": errors
        }

        return Response(response_data, status=status.HTTP_200_OK)
