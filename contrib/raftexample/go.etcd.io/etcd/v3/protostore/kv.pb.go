// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: kv.proto

package protostore

import (
	fmt "fmt"
	_ "github.com/gogo/protobuf/gogoproto"
	github_com_golang_protobuf_proto "github.com/golang/protobuf/proto"
	proto "github.com/golang/protobuf/proto"
	io "io"
	math "math"
	math_bits "math/bits"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type MyKV struct {
	Key   *string `protobuf:"bytes,1,req,name=key" json:"key,omitempty"`
	Value *string `protobuf:"bytes,2,req,name=value" json:"value,omitempty"`
}

func (m *MyKV) Reset()         { *m = MyKV{} }
func (m *MyKV) String() string { return proto.CompactTextString(m) }
func (*MyKV) ProtoMessage()    {}
func (*MyKV) Descriptor() ([]byte, []int) {
	return fileDescriptor_2216fe83c9c12408, []int{0}
}
func (m *MyKV) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *MyKV) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_MyKV.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *MyKV) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MyKV.Merge(m, src)
}
func (m *MyKV) XXX_Size() int {
	return m.Size()
}
func (m *MyKV) XXX_DiscardUnknown() {
	xxx_messageInfo_MyKV.DiscardUnknown(m)
}

var xxx_messageInfo_MyKV proto.InternalMessageInfo

func init() {
	proto.RegisterType((*MyKV)(nil), "protostore.MyKV")
}

func init() { proto.RegisterFile("kv.proto", fileDescriptor_2216fe83c9c12408) }

var fileDescriptor_2216fe83c9c12408 = []byte{
	// 155 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0xc8, 0x2e, 0xd3, 0x2b,
	0x28, 0xca, 0x2f, 0xc9, 0x17, 0xe2, 0x02, 0x53, 0xc5, 0x25, 0xf9, 0x45, 0xa9, 0x52, 0x22, 0xe9,
	0xf9, 0xe9, 0xf9, 0x60, 0xbe, 0x3e, 0x88, 0x05, 0x51, 0xa1, 0xa4, 0xc7, 0xc5, 0xe2, 0x5b, 0xe9,
	0x1d, 0x26, 0x24, 0xc0, 0xc5, 0x9c, 0x9d, 0x5a, 0x29, 0xc1, 0xa8, 0xc0, 0xa4, 0xc1, 0x19, 0x04,
	0x62, 0x0a, 0x89, 0x70, 0xb1, 0x96, 0x25, 0xe6, 0x94, 0xa6, 0x4a, 0x30, 0x81, 0xc5, 0x20, 0x1c,
	0x27, 0xe7, 0x13, 0x0f, 0xe5, 0x18, 0x2e, 0x3c, 0x94, 0x63, 0x38, 0xf1, 0x48, 0x8e, 0xf1, 0xc2,
	0x23, 0x39, 0xc6, 0x07, 0x8f, 0xe4, 0x18, 0x27, 0x3c, 0x96, 0x63, 0x98, 0xf1, 0x58, 0x8e, 0xe1,
	0xc2, 0x63, 0x39, 0x86, 0x1b, 0x8f, 0xe5, 0x18, 0xa2, 0x64, 0xd3, 0xf3, 0xf5, 0x52, 0x4b, 0x92,
	0x53, 0xf4, 0x32, 0xf3, 0xf5, 0x41, 0xb4, 0x7e, 0x99, 0xb1, 0x3e, 0xc2, 0x29, 0x80, 0x00, 0x00,
	0x00, 0xff, 0xff, 0xbe, 0x14, 0x2a, 0x02, 0xa1, 0x00, 0x00, 0x00,
}

func (m *MyKV) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *MyKV) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *MyKV) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Value == nil {
		return 0, new(github_com_golang_protobuf_proto.RequiredNotSetError)
	} else {
		i -= len(*m.Value)
		copy(dAtA[i:], *m.Value)
		i = encodeVarintKv(dAtA, i, uint64(len(*m.Value)))
		i--
		dAtA[i] = 0x12
	}
	if m.Key == nil {
		return 0, new(github_com_golang_protobuf_proto.RequiredNotSetError)
	} else {
		i -= len(*m.Key)
		copy(dAtA[i:], *m.Key)
		i = encodeVarintKv(dAtA, i, uint64(len(*m.Key)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintKv(dAtA []byte, offset int, v uint64) int {
	offset -= sovKv(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *MyKV) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Key != nil {
		l = len(*m.Key)
		n += 1 + l + sovKv(uint64(l))
	}
	if m.Value != nil {
		l = len(*m.Value)
		n += 1 + l + sovKv(uint64(l))
	}
	return n
}

func sovKv(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozKv(x uint64) (n int) {
	return sovKv(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *MyKV) Unmarshal(dAtA []byte) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowKv
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: MyKV: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: MyKV: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Key", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowKv
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthKv
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthKv
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			s := string(dAtA[iNdEx:postIndex])
			m.Key = &s
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000001)
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowKv
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthKv
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthKv
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			s := string(dAtA[iNdEx:postIndex])
			m.Value = &s
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000002)
		default:
			iNdEx = preIndex
			skippy, err := skipKv(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthKv
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}
	if hasFields[0]&uint64(0x00000001) == 0 {
		return new(github_com_golang_protobuf_proto.RequiredNotSetError)
	}
	if hasFields[0]&uint64(0x00000002) == 0 {
		return new(github_com_golang_protobuf_proto.RequiredNotSetError)
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipKv(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowKv
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowKv
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowKv
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthKv
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupKv
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthKv
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthKv        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowKv          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupKv = fmt.Errorf("proto: unexpected end of group")
)
