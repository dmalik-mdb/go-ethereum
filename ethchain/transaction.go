package ethchain

import (
	"bytes"
	"fmt"
	"github.com/ethereum/eth-go/ethutil"
	"github.com/obscuren/secp256k1-go"
	"math/big"
)

var ContractAddr = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

type Transaction struct {
	Nonce     uint64
	Recipient []byte
	Value     *big.Int
	Gas       *big.Int
	Gasprice  *big.Int
	Data      []string
	v         byte
	r, s      []byte

	// Indicates whether this tx is a contract creation transaction
	contractCreation bool
}

func NewContractCreationTx(value, gasprice *big.Int, data []string) *Transaction {
	return &Transaction{Value: value, Gasprice: gasprice, Data: data, contractCreation: true}
}

func NewTransactionMessage(to []byte, value, gasprice, gas *big.Int, data []string) *Transaction {
	return &Transaction{Recipient: to, Value: value, Gasprice: gasprice, Gas: gas, Data: data}
}

func NewTransactionFromBytes(data []byte) *Transaction {
	tx := &Transaction{}
	tx.RlpDecode(data)

	return tx
}

func NewTransactionFromValue(val *ethutil.Value) *Transaction {
	tx := &Transaction{}
	tx.RlpValueDecode(val)

	return tx
}

func (tx *Transaction) Hash() []byte {
	data := make([]interface{}, len(tx.Data))
	for i, val := range tx.Data {
		data[i] = val
	}

	preEnc := []interface{}{
		tx.Nonce,
		tx.Recipient,
		tx.Value,
		data,
	}

	return ethutil.Sha3Bin(ethutil.Encode(preEnc))
}

func (tx *Transaction) IsContract() bool {
	return bytes.Compare(tx.Recipient, ContractAddr) == 0
}

func (tx *Transaction) Signature(key []byte) []byte {
	hash := tx.Hash()

	sig, _ := secp256k1.Sign(hash, key)

	return sig
}

func (tx *Transaction) PublicKey() []byte {
	hash := tx.Hash()

	// If we don't make a copy we will overwrite the existing underlying array
	dst := make([]byte, len(tx.r))
	copy(dst, tx.r)

	sig := append(dst, tx.s...)
	sig = append(sig, tx.v-27)

	pubkey, _ := secp256k1.RecoverPubkey(hash, sig)

	return pubkey
}

func (tx *Transaction) Sender() []byte {
	pubkey := tx.PublicKey()

	// Validate the returned key.
	// Return nil if public key isn't in full format
	if pubkey[0] != 4 {
		return nil
	}

	return ethutil.Sha3Bin(pubkey[1:])[12:]
}

func (tx *Transaction) Sign(privk []byte) error {

	sig := tx.Signature(privk)

	tx.r = sig[:32]
	tx.s = sig[32:64]
	tx.v = sig[64] + 27

	return nil
}

func (tx *Transaction) RlpData() interface{} {
	data := []interface{}{tx.Nonce, tx.Value, tx.Gasprice}

	if !tx.contractCreation {
		data = append(data, tx.Recipient, tx.Gas)
	}

	return append(data, ethutil.NewSliceValue(tx.Data).Slice(), tx.v, tx.r, tx.s)
}

func (tx *Transaction) RlpValue() *ethutil.Value {
	return ethutil.NewValue(tx.RlpData())
}

func (tx *Transaction) RlpEncode() []byte {
	return tx.RlpValue().Encode()
}

func (tx *Transaction) RlpDecode(data []byte) {
	tx.RlpValueDecode(ethutil.NewValueFromBytes(data))
}

//   [ NONCE, VALUE, GASPRICE, TO, GAS, DATA, V, R, S ]
//["" "\x03\xe8" "" "\xaa" "\x03\xe8" [] '\x1c' "\x10C\x15\xfc\xe5\xd0\t\xe4\r\xe7\xefa\xf5aE\xd6\x14\xaed\xb5.\xf5\x18\xa1S_j\xe0A\xdc5U" "dQ\nqy\xf8\x17+\xbf\xd7Jx\xda-\xcb\xd7\xcfQ\x1bI\xb8_9\b\x80\xea듎i|\x1f"]
func (tx *Transaction) RlpValueDecode(decoder *ethutil.Value) {
	fmt.Println(decoder)
	tx.Nonce = decoder.Get(0).Uint()
	tx.Value = decoder.Get(1).BigInt()
	tx.Gasprice = decoder.Get(2).BigInt()

	// If the 4th item is a list(slice) this tx
	// is a contract creation tx
	if decoder.Get(3).IsList() {
		d := decoder.Get(3)
		tx.Data = make([]string, d.Len())
		for i := 0; i < d.Len(); i++ {
			tx.Data[i] = d.Get(i).Str()
		}

		tx.v = byte(decoder.Get(4).Uint())
		tx.r = decoder.Get(5).Bytes()
		tx.s = decoder.Get(6).Bytes()

		tx.contractCreation = true
	} else {
		tx.Recipient = decoder.Get(3).Bytes()
		tx.Gas = decoder.Get(4).BigInt()

		d := decoder.Get(5)
		tx.Data = make([]string, d.Len())
		for i := 0; i < d.Len(); i++ {
			tx.Data[i] = d.Get(i).Str()
		}

		tx.v = byte(decoder.Get(6).Uint())
		tx.r = decoder.Get(7).Bytes()
		tx.s = decoder.Get(8).Bytes()
	}
	/*
		tx.Nonce = decoder.Get(0).Uint()
		tx.Recipient = decoder.Get(1).Bytes()
		tx.Value = decoder.Get(2).BigInt()

		d := decoder.Get(3)
		tx.Data = make([]string, d.Len())
		for i := 0; i < d.Len(); i++ {
			tx.Data[i] = d.Get(i).Str()
		}

		tx.v = byte(decoder.Get(4).Uint())
		tx.r = decoder.Get(5).Bytes()
		tx.s = decoder.Get(6).Bytes()
	*/
}
