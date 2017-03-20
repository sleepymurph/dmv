pub struct VarianceCalc {
    k: i64,
    n: i64,
    sum: i64,
    sum_sq: i64,
}
impl VarianceCalc {
    pub fn new() -> Self {
        VarianceCalc {
            k: 0,
            n: 0,
            sum: 0,
            sum_sq: 0,
        }
    }
    fn panic_if_n_is_zero(&self) {
        if self.n == 0 {
            panic!("No values given");
        }
    }
    pub fn item(&mut self, item: i64) {
        if self.n == 0 {
            self.k = item;
        }
        self.n += 1;
        let imk = item - self.k;
        self.sum += imk;
        self.sum_sq += imk * imk;
    }
    pub fn items<'a, I>(&mut self, items: I)
        where I: Iterator<Item = i64>
    {
        for i in items {
            self.item(i);
        }
    }
    pub fn count(&self) -> i64 { self.n }
    pub fn mean(&self) -> f64 {
        self.panic_if_n_is_zero();
        let k = self.k as f64;
        let sum = self.sum as f64;
        let n = self.n as f64;
        k + sum / n
    }
    pub fn var(&self) -> f64 {
        self.panic_if_n_is_zero();
        let sum = self.sum as f64;
        let sum_sq = self.sum_sq as f64;
        let n = self.n as f64;
        (sum_sq - (sum.powi(2) / n)) / n
    }
    pub fn std(&self) -> f64 { self.var().sqrt() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_variance_calc_accuracy() {
        let mut calc = VarianceCalc::new();
        for x in 1..7 {
            calc.item(x);
        }
        assert_eq!(calc.mean(), 3.5, "mean");
        assert_eq!(calc.var(), 2.9166666666666665, "variance");
        assert_eq!(calc.std(), 1.707825127659933, "std dev");

        let input: Vec<i64> = vec![2, 4, 4, 4, 5, 5, 7, 9];
        let mut calc = VarianceCalc::new();
        calc.items(input.into_iter());
        assert_eq!(calc.mean(), 5.0, "mean");
        assert_eq!(calc.std(), 2.0, "std");
    }
}
