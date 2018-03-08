import os

if __name__ == "__main__":
    os.system('nohup python udp_ais_save.py bm > bm_ais.log &')
    os.system('nohup python udp_ais_save.py ys > ys_ais.log &')
    os.system('nohup python run_spark_moor.py > ais_moor.log &')
    os.system('nohup python3.5 vr_weather.py > weather.log &')
    print('start successfully!')
