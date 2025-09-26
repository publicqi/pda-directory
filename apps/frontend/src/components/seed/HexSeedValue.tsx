import TogglableSeedValue from './TogglableSeedValue';

interface HexSeedValueProps {
    decimalValue: string;
    rawHex: string;
}

const HexSeedValue = ({ decimalValue, rawHex }: HexSeedValueProps) => {
    return <TogglableSeedValue valueA={rawHex} valueB={decimalValue} label="Raw" />;
};

export default HexSeedValue;
