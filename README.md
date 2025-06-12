# Project Big Data 2

## Anggota Kelompok:
|             Nama              |     NRP    |
|-------------------------------|------------|
| Danar Bagus Rasendriya        | 5027231055 |
| Fadlillah Cantika S. H.          | 5027231042 |
| Syela Zeruya T. L.      | 5027231076 |
***
### Gambaran Umum:
![image](https://github.com/user-attachments/assets/c91da21a-2764-4d03-a3bc-6fb705749b0c)
Terdapat sebuah sistem Big Data dengan arsitektur seperti gambar di atas. Sistem tersebut berfungsi untuk menyimulasikan pemrosesan data stream menggunakan Kafka dan Apache Spark. Untuk kemudahan pemrosesan, Kafka Consumer tidak wajib menggunakan Spark Streaming. Alur yang diharapkan adalah sebagai berikut.
***
### Penjelasan Dataset
Dataset ini terdiri dari lebih dari satu juta komentar YouTube, terdapat label sentimen—Positif, Netral, atau Negatif. Komentar-komentar tersebut mencakup berbagai topik termasuk pemrograman, berita, olahraga, politik, dsb.
***
### Soal:
#### 1. Terdapat sebuah file dataset yang akan dibaca secara sekuensial oleh Kafka Producer.
link dataset: (https://www.kaggle.com/datasets/amaanpoonawala/youtube-comments-sentiment-dataset
)
***
#### 2. Kafka Producer akan mengirimkan data per baris ke Kafka Server seolah-olah sedang melakukan streaming. Proses ini dapat dilakukan dengan menambahkan jeda/sleep secara random agar data tidak dikirimkan secara langsung.

jalankan : ```python producer.py```

![image](https://github.com/user-attachments/assets/12cc4629-133e-4a00-b1f0-0c7b5ed4afa8)

***
#### 3. Kafka consumer membaca data yang ada di dalam Kafka server dan akan menyimpan data yang diterima dalam bentuk batch.
Batch dapat ditentukan berdasarkan:

- Jumlah data yang diterima
- Rentang waktu proses (window) Sehingga nanti akan didapatkan beberapa file dataset sesuai dengan batch yang dipilih.

jalankan : ```python consumer_batch.py```

![image](https://github.com/user-attachments/assets/7b3dc987-737a-48b2-adcb-88a79f2517c4)

![image](https://github.com/user-attachments/assets/a110b8bd-4ada-49ca-bd8f-7a831690c152)

***
#### 4. Spark script bertugas untuk melakukan training model sesuai dengan data yang masuk. Diharapkan ada beberapa model yang dihasilkan sesuai dengan jumlah data yang masuk. Kalian dapat menentukan sendiri berapa jumlah data yang diproses untuk tiap model.
Contoh:

A. Terdapat 3 model dengan skema sebagai berikut:
- Model 1: Menggunakan data selama 5 menit pertama atau 500.000 data pertama.
- Model 2: Menggunakan data selama 5 menit kedua atau 500.000 data kedua.
- Model 3: Menggunakan data selama 5 menit ketiga atau 500.000 data ketiga.

B. Terdapat 3 model dengan skema sebagai berikut:
- Model 1: 1/3 data pertama
- Model 2: 1/3 data pertama + 1/3 data kedua
- Model 3: 1/3 data pertama + 1/3 data kedua + 1/3 data terakhir (semua data)
***
Membuat Spark Script yang bertugas untuk training beberapa model sesuai data yang masuk.

Jalankan `train_models.py`, script akan membuat direktori `models` dan mulai training model. Setelahnya, model akan disimpan di direktori `models` pada subdirektori masing-masing.

Gambar Log Proses:
![Screenshot 2025-05-30 at 00 12 55](https://github.com/user-attachments/assets/bc99aa00-2667-4e88-b0db-01a1579c004f)


Gambar direktori `models` dan model yang dihasilkan:

![Screenshot 2025-05-30 at 00 30 47](https://github.com/user-attachments/assets/681bb7b2-f83f-4533-95c7-ff62afa86cda)
***
#### 5. Model-model yang dihasilkan akan digunakan di dalam API. Buatlah endpoint sesuai dengan jumlah model yang ada.
Membuat API dengan Endpoints yang sesuai dengan tiga model sebelumnya. API ini akan mengambil masukan teks dari pengguna dan mengembalikan prediksi sentimen.
Di sini saya menggunakan Flask untuk membuat API nya.

Saya membuat 3 endpoint:
- /predict/model1
- /predict/model2
- /predict/model3
![Screenshot 2025-05-30 at 02 01 26](https://github.com/user-attachments/assets/9c487a4c-c9fa-4516-8a70-b33daa95d4ee)


Jalankan `app.py`, output akan menunjukkan bahwa flask dijalankan dan models di-load:
![Screenshot 2025-05-30 at 01 11 20](https://github.com/user-attachments/assets/33e34646-874b-4f47-ace5-b260e53c30ab)

Proses Input dan Prediksi: Untuk setiap endpoints, ambil teks input, gunakan model yang dimuat untuk membuat prediksi sentimen, dan kembalikan hasilnya.

Melakukan tes menggunakan `curl`:
- Model 1
`curl -X POST -H "Content-Type: application/json" -d '{"text": "This is a wonderful video, thank you!"}' http://localhost:5000/predict/model1`

Output:
![Screenshot 2025-05-30 at 00 45 02](https://github.com/user-attachments/assets/c2e1e79c-1217-4c7b-860d-c78c0556f5b9)

- Model 2
`curl -X POST -H "Content-Type: application/json" -d '{"text": "I did not like this at all."}' http://localhost:5000/predict/model2`

Output:
![Screenshot 2025-05-30 at 00 45 42](https://github.com/user-attachments/assets/67cdb01e-0121-4216-a151-c1eff526399a)

- Model 3
`curl -X POST -H "Content-Type: application/json" -d '{"text": "This is just an average video, nothing special."}' http://localhost:5000/predict/model3`

Output:
![Screenshot 2025-05-30 at 00 46 13](https://github.com/user-attachments/assets/0262a2f2-98ea-4191-9e3a-574084978844)
***
#### 6. User akan melakukan request ke API. API akan memberikan respon sesuai dengan request user.
Misal:
- Apabila user melakukan request rekomendasi, maka input yang diperlukan adalah rating dari user dan response yang diberikan adalah daftar rekomendasi.
- Apabila modelnya adalah kasus clustering, maka response yang diberikan adalah ada di cluster mana data input dari user tersebut.
Jumlah API yang dibuat minimal sebanyak jumlah anggotanya (apabila ada 3 anggota, maka minimal membuat 3 api endpoint dengan fungsi berbeda)
***
- `/predict_sentiment_label`
  ![image](https://github.com/user-attachments/assets/fc7fd669-711a-4c4b-80f3-e448ec21cb57)

- `/predict_sentiment_probability`
  ![image](https://github.com/user-attachments/assets/f5f0626f-d25d-4f78-a226-952c77a7a07e)

- `/extract_keywords`
  ![image](https://github.com/user-attachments/assets/24bc43f2-ecd0-4986-aad0-4aaabecf6529)
***
