from kafka import KafkaConsumer
from json import loads
import os

consumer = KafkaConsumer(
    'amazonbeauty',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')))

folder_path = os.path.join(os.getcwd(), 'dataset')
limit = 500000      #limit jumlah data tiap model
counter = 1         #ini baris ke berapa
model = 1           #ini model ke berapa
model_limit = 3     #jumlah model
  
try:
    for message in consumer:
        if model > model_limit:
            writefile.close()
            break
        else:
            if counter > limit:
                counter = 1
                model += 1
                writefile.close()
            if model > model_limit:
                writefile.close()
                break
            if counter == 1:
                file_path = os.path.join(folder_path, ('model-' + str(model) + '.txt'))
                writefile = open(file_path, "w", encoding="utf-8")
            message = message.value
            writefile.write(message)
            print('current batch : ' + str(model) + ' current data for this batch : ' + str(counter))
            counter += 1
except KeyboardInterrupt:
    writefile.close()
    print('Keyboard Interrupt called by user, exiting.....')