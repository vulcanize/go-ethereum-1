// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package mocks

import (
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/indexing"
)

// Builder is a mock state diff builder
type Builder struct {
	Args         indexing.Args
	Params       indexing.Params
	stateDiff    indexing.StateObject
	block        *types.Block
	stateTrie    indexing.StateObject
	builderError error
}

// BuildStateDiffObject mock method
func (builder *Builder) BuildStateDiffObject(args indexing.Args, params indexing.Params) (indexing.StateObject, error) {
	builder.Args = args
	builder.Params = params

	return builder.stateDiff, builder.builderError
}

// BuildStateTrieObject mock method
func (builder *Builder) BuildStateTrieObject(block *types.Block) (indexing.StateObject, error) {
	builder.block = block

	return builder.stateTrie, builder.builderError
}

// SetStateDiffToBuild mock method
func (builder *Builder) SetStateDiffToBuild(stateDiff indexing.StateObject) {
	builder.stateDiff = stateDiff
}

// SetBuilderError mock method
func (builder *Builder) SetBuilderError(err error) {
	builder.builderError = err
}
