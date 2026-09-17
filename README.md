# BÀI TẬP 5: TÍCH HỢP WEBCLIENT & KAFKA – XỬ LÝ LUỒNG SỰ KIỆN THÔNG BÁO THÔNG MINH

## 1. Mục tiêu
- Kết hợp WebClient (Non-blocking) và Kafka Consumer trong cùng một luồng.
- Sử dụng Retry, Timeout, Fallback cho WebClient.
- Đảm bảo tính Lũy đẳng (Idempotency).
- Tích hợp DLQ với WebFlux.

## 2. Báo cáo cấu hình và Xử lý lỗi

### 2.1. Cấu hình Timeout & Retry WebClient
Đã tạo cấu hình Bean `WebClient` với thời gian Timeout là 3 giây (kết nối và đọc/ghi dữ liệu). Lệnh gọi API có sử dụng cơ chế `.retryWhen(Retry.fixedDelay(2, Duration.ofSeconds(1)))` để đảm bảo thực hiện lại tự động 2 lần (mỗi lần cách nhau 1s) nếu gặp lỗi liên kết mạng hoặc HTTP 5xx.

### 2.2. BUG-05: User Preference API thất bại
**Cách xử lý:** Sử dụng `.onErrorResume(...)` ngay sau hàm `.retryWhen()` khi gọi Preference API. Nếu API báo lỗi kết nối hoặc mã 5xx (và quá số lần retry), ứng dụng sẽ tự động in log báo mức Cảnh báo (WARNING) và trả về kết quả dự phòng là `"EMAIL"` mà không làm gián đoạn luồng xử lý chính.

### 2.3. BUG-06: Đảm bảo tính lũy đẳng (Idempotency) khi xử lý trùng lặp
**Cách xử lý:** Đã khai báo một tập hợp `ConcurrentHashMap<String, Boolean> processedOrders` để lưu các `orderId` đã nhận. Ngay đầu hàm Consumer, kiểm tra nếu `putIfAbsent(orderId, true)` trả về giá trị khác null thì có nghĩa đơn hàng này đã từng được đưa vào Map, suy ra đó là luồng lặp lại. Lúc này bỏ qua (`Skip`) không gọi API và return ngay lập tức. Kafka sẽ tự động commit offset sau hàm đó.

### 2.4. BUG-07 & REQ-01: Xử lý Non-blocking và DLQ
**Cách xử lý:**
Toàn bộ logic gửi WebClient được xâu chuỗi thông qua `.flatMap(...)` và được gọi chạy nền bằng hàm `.subscribe()`. Hàm Listener không hề bị khóa (Không dùng `.block()`). 
Tuy nhiên, do hàm không bị khóa nên lỗi xảy ra (ví dụ gửi Email hỏng 2 lần) sẽ bay về callback `error -> {}` của phương thức `subscribe()`, chứ không ném Exception ra thread của Spring Kafka. 
Do đó, trong khối `error`, hệ thống đã in log mức ERROR: `"Đã đẩy order {orderId} vào DLQ do lỗi gửi thông báo"` và chủ động dùng `kafkaTemplate.send("storex-order-events.DLQ", orderId, event);` để chuyển tiếp sự kiện đó sang Topic DLQ.
