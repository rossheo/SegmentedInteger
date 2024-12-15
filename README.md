구현 목표
 - 데이터 최적화를 목표로 하지 않고 이식 가능한 코드를 목표로 한다.
 - 최적화 코드를 추가하면 이해하기 어려워지고, 다른 언어로 동일한 로직을 구현하기 어려워진다.
 - 데이터는 Google protobuf를 활용
 - 코드는 단순한 로직만 사용

Google protobuf의 가변 인코딩은 다음과 같은 특성이 있다.
 - https://protobuf.dev/programming-guides/encoding/#varints
 - 7bit payload를 사용하여 가변 크기를 지원한다.

Segment
 - 내부적으로 Segment2M과 Segment64로 처리한다.
   - Segment2M (2,000,000를 의미하며, 시작 정수형 + 1 ~ 1,999,999 증분 정수형으로 처리)
   - Segment64 (64를 의미하며, 시작 정수형 + 1 ~ 63 증분 정수형을 bit 단위로 처리)
