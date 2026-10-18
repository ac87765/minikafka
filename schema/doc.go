// Package schema 用测试证明 proto/ 里的记录格式能安全演进。
//
// 这里没有业务代码，只有测试，它们是阶段 2 的「保证」的出处：
//
//	wire_test.go       Protobuf 线上格式长什么样（手工核对每一个字节）
//	evolution_test.go  v1 和 v2 互相读对方写的数据（向后兼容、向前兼容）
//	                   以及一个错误演进会怎样悄无声息地丢数据
package schema
