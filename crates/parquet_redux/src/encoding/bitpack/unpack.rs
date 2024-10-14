use num::traits::FromBytes;
use num::PrimInt;

pub fn unpack8(input: &[u8], output: &mut [u8; 8], num_bits: usize) {
    unpack_jump::<u8, 1, 8>(input, output, num_bits);
}

pub fn unpack16(input: &[u8], output: &mut [u16; 16], num_bits: usize) {
    unpack_jump::<u16, 2, 16>(input, output, num_bits);
}

pub fn unpack32(input: &[u8], output: &mut [u32; 32], num_bits: usize) {
    unpack_jump::<u32, 4, 32>(input, output, num_bits);
}

pub fn unpack64(input: &[u8], output: &mut [u64; 64], num_bits: usize) {
    unpack_jump::<u64, 8, 64>(input, output, num_bits);
}

fn unpack<const NUM_BITS: usize, T, const BYTES: usize, const BITS: usize>(
    input: &[u8],
    output: &mut [T; BITS],
) where
    T: PrimInt + Default + FromBytes<Bytes = [u8; BYTES]>,
{
    if NUM_BITS == 0 {
        for out in output.iter_mut() {
            *out = T::zero();
        }
        return;
    }

    assert!(NUM_BITS <= BYTES * 8);

    let mask = if NUM_BITS == BITS {
        T::max_value()
    } else {
        (T::one() << NUM_BITS) - T::one()
    };

    assert!(input.len() >= NUM_BITS * BYTES);

    for i in 0..BITS {
        let start_bit = i * NUM_BITS;
        let end_bit = start_bit + NUM_BITS;

        let start_bit_offset = start_bit % BITS;
        let end_bit_offset = end_bit % BITS;
        let start_byte = start_bit / BITS;
        let end_byte = end_bit / BITS;

        if start_byte != end_byte && end_bit_offset != 0 {
            let val = from_bytes::<T, BYTES>(start_byte, input);
            let a = val >> start_bit_offset;
            let val = from_bytes::<T, BYTES>(end_byte, input);
            let b = val << (NUM_BITS - end_bit_offset);

            output[i] = a | (b & mask);
        } else {
            let val = from_bytes::<T, BYTES>(start_byte, input);
            output[i] = (val >> start_bit_offset) & mask;
        }
    }
}

fn from_bytes<T, const BYTES: usize>(idx: usize, bytes: &[u8]) -> T
where
    T: FromBytes<Bytes = [u8; BYTES]>,
{
    let bytes: [u8; BYTES] = bytes[idx * BYTES..idx * BYTES + BYTES].try_into().unwrap();
    T::from_le_bytes(&bytes)
}

fn unpack_jump<T, const BYTES: usize, const BITS: usize>(
    input: &[u8],
    output: &mut [T; BITS],
    num_bits: usize,
) where
    T: PrimInt + Default + FromBytes<Bytes = [u8; BYTES]>,
{
    match num_bits {
        0 => unpack::<0, T, BYTES, BITS>(input, output),
        1 => unpack::<1, T, BYTES, BITS>(input, output),
        2 => unpack::<2, T, BYTES, BITS>(input, output),
        3 => unpack::<3, T, BYTES, BITS>(input, output),
        4 => unpack::<4, T, BYTES, BITS>(input, output),
        5 => unpack::<5, T, BYTES, BITS>(input, output),
        6 => unpack::<6, T, BYTES, BITS>(input, output),
        7 => unpack::<7, T, BYTES, BITS>(input, output),
        8 => unpack::<8, T, BYTES, BITS>(input, output),
        9 => unpack::<9, T, BYTES, BITS>(input, output),
        10 => unpack::<10, T, BYTES, BITS>(input, output),
        11 => unpack::<11, T, BYTES, BITS>(input, output),
        12 => unpack::<12, T, BYTES, BITS>(input, output),
        13 => unpack::<13, T, BYTES, BITS>(input, output),
        14 => unpack::<14, T, BYTES, BITS>(input, output),
        15 => unpack::<15, T, BYTES, BITS>(input, output),
        16 => unpack::<16, T, BYTES, BITS>(input, output),
        17 => unpack::<17, T, BYTES, BITS>(input, output),
        18 => unpack::<18, T, BYTES, BITS>(input, output),
        19 => unpack::<19, T, BYTES, BITS>(input, output),
        20 => unpack::<20, T, BYTES, BITS>(input, output),
        21 => unpack::<21, T, BYTES, BITS>(input, output),
        22 => unpack::<22, T, BYTES, BITS>(input, output),
        23 => unpack::<23, T, BYTES, BITS>(input, output),
        24 => unpack::<24, T, BYTES, BITS>(input, output),
        25 => unpack::<25, T, BYTES, BITS>(input, output),
        26 => unpack::<26, T, BYTES, BITS>(input, output),
        27 => unpack::<27, T, BYTES, BITS>(input, output),
        28 => unpack::<28, T, BYTES, BITS>(input, output),
        29 => unpack::<29, T, BYTES, BITS>(input, output),
        30 => unpack::<30, T, BYTES, BITS>(input, output),
        31 => unpack::<31, T, BYTES, BITS>(input, output),
        32 => unpack::<32, T, BYTES, BITS>(input, output),
        33 => unpack::<33, T, BYTES, BITS>(input, output),
        34 => unpack::<34, T, BYTES, BITS>(input, output),
        35 => unpack::<35, T, BYTES, BITS>(input, output),
        36 => unpack::<36, T, BYTES, BITS>(input, output),
        37 => unpack::<37, T, BYTES, BITS>(input, output),
        38 => unpack::<38, T, BYTES, BITS>(input, output),
        39 => unpack::<39, T, BYTES, BITS>(input, output),
        40 => unpack::<40, T, BYTES, BITS>(input, output),
        41 => unpack::<41, T, BYTES, BITS>(input, output),
        42 => unpack::<42, T, BYTES, BITS>(input, output),
        43 => unpack::<43, T, BYTES, BITS>(input, output),
        44 => unpack::<44, T, BYTES, BITS>(input, output),
        45 => unpack::<45, T, BYTES, BITS>(input, output),
        46 => unpack::<46, T, BYTES, BITS>(input, output),
        47 => unpack::<47, T, BYTES, BITS>(input, output),
        48 => unpack::<48, T, BYTES, BITS>(input, output),
        49 => unpack::<49, T, BYTES, BITS>(input, output),
        50 => unpack::<50, T, BYTES, BITS>(input, output),
        51 => unpack::<51, T, BYTES, BITS>(input, output),
        52 => unpack::<52, T, BYTES, BITS>(input, output),
        53 => unpack::<53, T, BYTES, BITS>(input, output),
        54 => unpack::<54, T, BYTES, BITS>(input, output),
        55 => unpack::<55, T, BYTES, BITS>(input, output),
        56 => unpack::<56, T, BYTES, BITS>(input, output),
        57 => unpack::<57, T, BYTES, BITS>(input, output),
        58 => unpack::<58, T, BYTES, BITS>(input, output),
        59 => unpack::<59, T, BYTES, BITS>(input, output),
        60 => unpack::<60, T, BYTES, BITS>(input, output),
        61 => unpack::<61, T, BYTES, BITS>(input, output),
        62 => unpack::<62, T, BYTES, BITS>(input, output),
        63 => unpack::<63, T, BYTES, BITS>(input, output),
        64 => unpack::<64, T, BYTES, BITS>(input, output),
        _ => panic!("invalid num_bits {}", num_bits),
    }
}
