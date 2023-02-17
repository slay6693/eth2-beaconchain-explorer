package db

import (
	"encoding/binary"
	"eth2-exporter/types"
	"eth2-exporter/utils"
	"fmt"
	"sort"
	"strconv"
	"strings"

	gcp_bigtable "cloud.google.com/go/bigtable"
)

func (bigtable *Bigtable) SaveEpoch(data *types.EpochData) error {

	tsZero := gcp_bigtable.Timestamp(0)

	muts := make(map[uint64]map[uint64]*gcp_bigtable.Mutation)

	epochReversePadded := reversedPaddedEpoch(data.Epoch)

	day := data.Epoch / utils.EpochsPerDay()

	muts[day] = make(map[uint64]*gcp_bigtable.Mutation)
	muts[day+1] = make(map[uint64]*gcp_bigtable.Mutation)
	if day > 0 {
		muts[day-1] = make(map[uint64]*gcp_bigtable.Mutation)
	}
	// create relevant mutations for each validator & validator balances
	for _, validator := range data.Validators {
		muts[day][validator.Index] = gcp_bigtable.NewMutation()

		// set validator balances
		balanceEncoded := make([]byte, 8)
		binary.LittleEndian.PutUint64(balanceEncoded, validator.Balance)

		effectiveBalanceEncoded := make([]byte, 8)
		binary.LittleEndian.PutUint64(effectiveBalanceEncoded, validator.EffectiveBalance)

		combined := append(balanceEncoded, effectiveBalanceEncoded...)
		muts[day][validator.Index].Set(VALIDATOR_BALANCES_FAMILY, epochReversePadded, tsZero, combined)
	}

	// create attestation assignment mutations
	validatorsPerSlot := make(map[uint64][]uint64)
	for key, validator := range data.ValidatorAssignmentes.AttestorAssignments {
		keySplit := strings.Split(key, "-")

		attesterslot, err := strconv.ParseUint(keySplit[0], 10, 64)
		if err != nil {
			return err
		}

		if validatorsPerSlot[attesterslot] == nil {
			validatorsPerSlot[attesterslot] = make([]uint64, 0, len(data.ValidatorAssignmentes.AttestorAssignments)/int(utils.Config.Chain.Config.SlotsPerEpoch))
		}
		validatorsPerSlot[attesterslot] = append(validatorsPerSlot[attesterslot], validator)
	}

	for slot, validators := range validatorsPerSlot {
		slotEncoded := make([]byte, 8)
		binary.LittleEndian.PutUint64(slotEncoded, slot)
		for _, validator := range validators {
			muts[day][validator].Set(ATTESTATIONS_FAMILY, epochReversePadded, tsZero, slotEncoded)
		}
	}

	// create proposer assignment mutations
	for slot, validator := range data.ValidatorAssignmentes.ProposerAssignments {
		slotDay := utils.DayOfSlot(slot)

		if muts[slotDay][validator] == nil {
			muts[slotDay][validator] = gcp_bigtable.NewMutation()
		}
		muts[day][validator].Set(PROPOSALS_FAMILY, fmt.Sprintf("%s:%d", epochReversePadded, slot), tsZero, []byte{})
	}

	// create sync assignment mutations
	attestationsBySlot := make(map[uint64]map[uint64]uint64) //map[attestedSlot]map[validator]includedSlot

	slots := make([]uint64, 0, len(data.Blocks))
	for slot := range data.Blocks {
		slots = append(slots, slot)
	}
	sort.Slice(slots, func(i, j int) bool {
		return slots[i] < slots[j]
	})

	for _, slot := range slots {
		for _, b := range data.Blocks[slot] {

			// create proposer mutation
			if len(b.BlockRoot) == 32 { // skip dummy blocks
				muts[day][b.Proposer].Set(PROPOSALS_FAMILY, fmt.Sprintf("%s:%d", epochReversePadded, slot), gcp_bigtable.Timestamp((max_block_number-b.Slot)*1000), []byte{})
			}

			// create sync duties mutations
			if b.Status == 2 {
				// do nothing
			} else if b.SyncAggregate != nil && len(b.SyncAggregate.SyncCommitteeValidators) > 0 {
				bitLen := len(b.SyncAggregate.SyncCommitteeBits) * 8
				valLen := len(b.SyncAggregate.SyncCommitteeValidators)
				if bitLen < valLen {
					return fmt.Errorf("error getting sync_committee participants: bitLen != valLen: %v != %v", bitLen, valLen)
				}
				for i, valIndex := range b.SyncAggregate.SyncCommitteeValidators {
					participated := utils.BitAtVector(b.SyncAggregate.SyncCommitteeBits, i)
					if participated {
						muts[day][valIndex].Set(SYNC_COMMITTEES_FAMILY, fmt.Sprintf("%s:%d", epochReversePadded, slot), gcp_bigtable.Timestamp((max_block_number-slot)*1000), []byte{})
					} else {
						muts[day][valIndex].Set(SYNC_COMMITTEES_FAMILY, fmt.Sprintf("%s:%d", epochReversePadded, slot), gcp_bigtable.Timestamp(0), []byte{})
					}
				}
			}

			// prepare the attestation duties
			for _, a := range b.Attestations {
				for _, validator := range a.Attesters {
					inclusionSlot := slot
					attestedSlot := a.Data.Slot
					if attestationsBySlot[attestedSlot] == nil {
						attestationsBySlot[attestedSlot] = make(map[uint64]uint64)
					}

					if attestationsBySlot[attestedSlot][validator] == 0 || inclusionSlot < attestationsBySlot[attestedSlot][validator] {
						attestationsBySlot[attestedSlot][validator] = inclusionSlot
					}
				}
			}
		}
	}

	// create attestation mutations
	for attestedSlot, inclusions := range attestationsBySlot {
		attestedSlotDay := utils.DayOfSlot(attestedSlot)
		attestedSlotEncoded := make([]byte, 8)
		binary.LittleEndian.PutUint64(attestedSlotEncoded, attestedSlot)
		for validator, inclusionSlot := range inclusions {
			if muts[attestedSlotDay][validator] == nil {
				muts[attestedSlotDay][validator] = gcp_bigtable.NewMutation()
			}
			muts[attestedSlotDay][validator].Set(ATTESTATIONS_FAMILY, epochReversePadded, gcp_bigtable.Timestamp((max_block_number-inclusionSlot)*1000), attestedSlotEncoded)
		}
	}

	bulkMut := &types.BulkMutations{
		Keys: make([]string, 0, len(data.Validators)),
		Muts: make([]*gcp_bigtable.Mutation, 0, len(data.Validators)),
	}

	for day, validators := range muts {
		for validatorIndex, mutation := range validators {

			bulkMut.Keys = append(bulkMut.Keys, fmt.Sprintf("%s:v:%s:d:%s", bigtable.chainId, reverseValidatorIndex(validatorIndex), reversedPaddedDay(day)))
			bulkMut.Muts = append(bulkMut.Muts, mutation)
		}
	}

	return bigtable.WriteBulk(bulkMut, bigtable.tableBeaconchain)
}
