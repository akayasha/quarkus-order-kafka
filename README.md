# Quarkus Order Kafka

Aplikasi ini adalah contoh implementasi event-driven architecture menggunakan Quarkus, Apache Kafka, dan PostgreSQL. Tujuannya sederhana: ketika ada order masuk lewat REST API, order tersebut dikirim ke Kafka, diproses, diklasifikasikan, lalu hasilnya dipublikasikan ke topic lain. Kalau pemrosesan gagal, ada mekanisme retry sampai 3 kali sebelum message dibuang ke Dead Letter Queue (DLQ).

---

## Gambaran Alur

```text
POST /api/orders
     |
     v
  PostgreSQL (status: PENDING)
     |
     v
  Kafka topic: orders
     |
     v
  OrderConsumer (klasifikasi HIGH/NORMAL)
     |
     +-- Sukses --> Kafka topic: orders-processed + update DB (PROCESSED)
     |
     +-- Gagal --> retry sampai 3x --> Kafka topic: orders-dlq + update DB (FAILED)
```

Klasifikasi order berdasarkan jumlah amount:
- Amount >= 1.000.000 → dikategorikan HIGH
- Amount < 1.000.000 → dikategorikan NORMAL

---

## Teknologi yang Digunakan

- Java 17
- Quarkus 3.8.1
- Apache Kafka (SmallRye Reactive Messaging)
- PostgreSQL 15
- Hibernate ORM with Panache
- Maven

---

## Struktur Project

```text
quarkus-order-kafka/
├── src/
│   └── main/
│       ├── java/com/example/order/
│       │   ├── consumer/
│       │   │   └── OrderConsumer.java       # Konsumsi dari topic orders, proses & kirim ke orders-processed
│       │   ├── deserializer/
│       │   │   └── OrderEventDeserializer.java
│       │   ├── model/
│       │   │   ├── Order.java               # Entity JPA yang disimpan di PostgreSQL
│       │   │   ├── OrderEvent.java          # DTO untuk Kafka message
│       │   │   ├── OrderRequest.java        # DTO untuk request body REST API
│       │   │   └── ApiResponse.java         # Standar response API
│       │   ├── producer/
│       │   │   └── OrderProducer.java       # Kirim ke topic orders dan orders-dlq
│       │   ├── repository/
│       │   │   └── OrderRepository.java
│       │   ├── service/
│       │   │   └── OrderService.java        # Logic database transaction
│       │   └── resource/
│       │       └── OrderResource.java       # REST endpoint POST dan GET
│       └── resources/
│           └── application.properties
├── docker-compose.yml
└── pom.xml
```

---

## Kafka Topics

| Topic | Keterangan |
|---|---|
| `orders` | Order baru masuk dari REST API |
| `orders-processed` | Order yang berhasil diproses beserta prioritasnya |
| `orders-dlq` | Order yang gagal diproses setelah 3x retry atau cacat format (Poison Pill) |

---

## Cara Menjalankan

### 1. Jalankan infrastruktur dulu

Pastikan Docker sudah berjalan, lalu jalankan:

```bash
docker-compose up -d
```

Ini akan menjalankan PostgreSQL, Zookeeper, Kafka, dan Kafka-UI secara bersamaan. Tunggu sekitar 15-20 detik sampai Kafka siap. Cek statusnya dengan:

```bash
docker-compose ps
```

### 2. Jalankan aplikasi Quarkus

```bash
./mvnw quarkus:dev
```

Aplikasi akan berjalan di port `8080`.

---

## Menggunakan REST API

### Buat Order Baru (Skenario Sukses / Normal Flow)

Endpoint ini berada di `/api/orders`.

```bash
curl --location 'http://localhost:8080/api/orders' \
--header 'Content-Type: application/json' \
--data '{
    "customerId": "cust-001",
    "productName": "Laptop Gaming",
    "amount": 2500000
}'
```

**Ekspektasi:** Order dengan amount 2.500.000 akan diklasifikasikan sebagai `HIGH`. Database akan ter-update menjadi `PROCESSED`.

### Ambil Semua Order

```bash
curl --location 'http://localhost:8080/api/orders'
```

### Cek Status Koneksi Kafka

```bash
curl --location 'http://localhost:8080/api/orders/kafka/status'
```

---

## Mekanisme Retry dan Dead Letter Queue (DLQ)

Ketika pemrosesan order gagal di `OrderConsumer`, sistem akan:

1. Menangkap error (`Throwable`).
2. Menambahkan `retryCount` pada event.
3. Mengirim ulang event tersebut ke topic `orders`.
4. Proses diulang maksimal **3 kali**.
5. Jika setelah 3 kali masih gagal, event dikirim ke topic `orders-dlq` dan status order di database diupdate menjadi `FAILED`.

### Menguji Simulasi Error (Error Flow)

Untuk mengetes apakah Retry dan DLQ berjalan dengan baik, kita sengaja menanamkan "Trigger Error" di Consumer. Kirimkan order dengan `productName` diisi **"ERROR-DLQ"**:

```bash
curl --location 'http://localhost:8080/api/orders' \
--header 'Content-Type: application/json' \
--data '{
    "customerId": "cust-error",
    "productName": "ERROR-DLQ",
    "amount": 150000
}'
```

**Apa yang terjadi?**
1. API mengembalikan HTTP `201 Created` (Karena data valid secara format).
2. Di log Quarkus, kamu akan melihat aplikasi mencoba me-retry sebanyak 3 kali: `Retrying order... attempt 1/3`, dst.
3. Setelah gagal 3 kali, akan muncul log `Routing to DLQ`.
4. Lakukan `GET /api/orders`, pesanan ini akan berstatus `FAILED`.
5. Buka Kafka UI (`http://localhost:8090`), pesan tersebut sudah berada di topic `orders-dlq`.

### Kenapa Harus Pakai "Simulasi Error" dan Bukan Error Sungguhan (Misal: amount = 0)?

Dalam *Best Practice* arsitektur perangkat lunak, kita menerapkan prinsip **Fail-Fast** (Gagal Lebih Awal). Artinya, validasi data yang sudah pasti salah—seperti *amount* kurang dari 0 atau nama kosong—**harus ditolak di pintu masuk (REST API)**, bukan dibiarkan masuk ke Kafka.

Alasan kita menggunakan trigger "Simulasi":
1. **Validasi API**: Jika kita menggunakan `amount: 0`, REST API akan langsung membalas `400 Bad Request`. Pesan tidak akan pernah mencapai Kafka, sehingga kita tidak bisa menguji fitur DLQ kita.
2. **Fungsi Asli DLQ**: DLQ (*Dead Letter Queue*) dirancang untuk menampung *"Unpredictable Runtime Error"* (Error sistem yang tidak terprediksi). Contoh di dunia nyata: Database *timeout*, API pihak ketiga *down*, atau format data JSON mendadak rusak dari *service* lain.
3. **Mencegah Pesan Sampah**: Kita tidak ingin membebani Kafka dan sistem *Consumer* dengan data sampah yang secara logika bisnis sudah salah sejak awal.

Oleh karena itu, menyuntikkan *logic* simulasi (seperti `productName: "ERROR-DLQ"`) adalah cara paling aman untuk memastikan mekanisme *Retry* dan DLQ kita berfungsi tanpa merusak validasi bisnis di sisi API.

---

## Monitoring Kafka

Kafka UI tersedia di http://localhost:8090 setelah `docker-compose` dijalankan. Dari sana bisa dilihat message di setiap topic, consumer group, dan offset.

Atau bisa juga lewat terminal:

```bash
# Lihat message di topic orders-dlq
docker exec kafka kafka-console-consumer \
  --topic orders-dlq \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

---

## Menghentikan Aplikasi

```bash
# Hentikan Quarkus: tekan Ctrl+C di terminal

# Hentikan dan hapus container Docker
docker-compose down

# Hentikan dan hapus container beserta data volume (data PostgreSQL terhapus)
docker-compose down -v
```