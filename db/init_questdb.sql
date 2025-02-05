CREATE TABLE IF NOT EXISTS stock_data (
    stock_symbol SYMBOL,   
    signal STRING,    
    local_time TIMESTAMP, 
    open DOUBLE,          
    close DOUBLE,        
    high DOUBLE,          
    low DOUBLE,           
    volume DOUBLE,          
    SMA_5 DOUBLE,         
    EMA_10 DOUBLE,        
    delta DOUBLE,
    gain DOUBLE,
    loss DOUBLE,
    avg_gain_10 DOUBLE,
    avg_loss_10 DOUBLE,
    rs DOUBLE,
    RSI_10 DOUBLE
)
TIMESTAMP(local_time)
PARTITION BY DAY;