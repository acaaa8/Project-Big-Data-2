# Project Big Data-2

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
(Here)
***
### Soal:
#### 1. Terdapat sebuah file dataset yang akan dibaca secara sekuensial oleh Kafka Producer.
(link dataset: (here))
***
#### 2. Kafka Producer akan mengirimkan data per baris ke Kafka Server seolah-olah sedang melakukan streaming. Proses ini dapat dilakukan dengan menambahkan jeda/sleep secara random agar data tidak dikirimkan secara langsung.
(Penjelasan/Dokumentasi)
***
#### 3. Kafka consumer membaca data yang ada di dalam Kafka server dan akan menyimpan data yang diterima dalam bentuk batch.
Batch dapat ditentukan berdasarkan:
```
Jumlah data yang diterima
```
```
Rentang waktu proses (window) Sehingga nanti akan didapatkan beberapa file dataset sesuai dengan batch yang dipilih.
```
