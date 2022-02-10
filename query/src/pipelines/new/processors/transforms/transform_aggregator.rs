use std::alloc::Layout;
use std::sync::Arc;
use tonic::Code::Unimplemented;
use common_datablocks::{DataBlock, HashMethodKeysU16, HashMethodKeysU32, HashMethodKeysU64, HashMethodKeysU8, HashMethodKind, HashMethodSerializer};
use common_datavalues2::DataSchemaRef;
use common_exception::{ErrorCode, Result};
use common_functions::aggregates::{AggregateFunctionRef, get_layout_offsets};
use common_planners::Expression;
use crate::pipelines::new::processors::port::{InputPort, OutputPort};
use crate::pipelines::new::processors::{AggregatorParams, AggregatorTransformParams, Processor};
use crate::pipelines::new::processors::processor::{Event, ProcessorPtr};
use crate::pipelines::new::processors::transforms::aggregator::{FinalSingleKeyAggregator, KeysU16PartialAggregator, KeysU32PartialAggregator, KeysU64PartialAggregator, KeysU8PartialAggregator, PartialAggregator, PartialSingleKeyAggregator, SerializerPartialAggregator};
use crate::pipelines::new::processors::transforms::transform::Transform;


pub struct TransformAggregator;

impl TransformAggregator {
    pub fn try_create_final(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        transform_params: AggregatorTransformParams,
    ) -> Result<ProcessorPtr> {
        let aggregator_params = transform_params.aggregator_params;

        if aggregator_params.group_columns_name.is_empty() {
            return AggregatorTransform::create(
                input_port,
                output_port,
                FinalSingleKeyAggregator::try_create(&aggregator_params)?,
            );
        }

        unimplemented!()
    }

    pub fn try_create_partial(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        transform_params: AggregatorTransformParams,
    ) -> Result<ProcessorPtr> {
        let aggregator_params = transform_params.aggregator_params;

        if aggregator_params.group_columns_name.is_empty() {
            return AggregatorTransform::create(
                input_port,
                output_port,
                PartialSingleKeyAggregator::try_create(&aggregator_params)?,
            );
        }

        match aggregator_params.aggregate_functions.is_empty() {
            true => match transform_params.method {
                HashMethodKind::KeysU8(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU8PartialAggregator::<false>::create(method, aggregator_params),
                ),
                HashMethodKind::KeysU16(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU16PartialAggregator::<false>::create(method, aggregator_params),
                ),
                HashMethodKind::KeysU32(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU32PartialAggregator::<false>::create(method, aggregator_params),
                ),
                HashMethodKind::KeysU64(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU64PartialAggregator::<false>::create(method, aggregator_params),
                ),
                HashMethodKind::Serializer(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    SerializerPartialAggregator::<false>::create(method, aggregator_params),
                ),
            }
            false => match transform_params.method {
                HashMethodKind::KeysU8(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU8PartialAggregator::<true>::create(method, aggregator_params),
                ),
                HashMethodKind::KeysU16(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU16PartialAggregator::<true>::create(method, aggregator_params),
                ),
                HashMethodKind::KeysU32(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU32PartialAggregator::<true>::create(method, aggregator_params),
                ),
                HashMethodKind::KeysU64(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU64PartialAggregator::<true>::create(method, aggregator_params),
                ),
                HashMethodKind::Serializer(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    SerializerPartialAggregator::<true>::create(method, aggregator_params),
                ),
            }
        }
    }
}

pub trait Aggregator: Sized + Send {
    const NAME: &'static str;

    fn consume(&mut self, data: DataBlock) -> Result<()>;
    fn generate(&mut self) -> Result<Option<DataBlock>>;
}


enum AggregatorTransform<TAggregator: Aggregator> {
    ConsumeData(ConsumeState<TAggregator>),
    Generate(GenerateState<TAggregator>),
    Finished,
}

impl<TAggregator: Aggregator + 'static> AggregatorTransform<TAggregator> {
    pub fn create(input_port: Arc<InputPort>, output_port: Arc<OutputPort>, inner: TAggregator) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(AggregatorTransform::<TAggregator>::ConsumeData(
            ConsumeState {
                inner,
                input_port,
                output_port,
                input_data_block: None,
            }
        ))))
    }

    pub fn to_generate(self) -> Result<Self> {
        match self {
            AggregatorTransform::ConsumeData(s) => Ok(AggregatorTransform::Generate(
                GenerateState {
                    inner: s.inner,
                    is_finished: false,
                    input_port: s.input_port,
                    output_port: s.output_port,
                    output_data_block: None,
                }
            )),
            _ => Err(ErrorCode::LogicalError(""))
        }
    }
}

impl<TAggregator: Aggregator + 'static> Processor for AggregatorTransform<TAggregator> {
    fn name(&self) -> &'static str {
        TAggregator::NAME
    }

    fn event(&mut self) -> Result<Event> {
        match self {
            AggregatorTransform::Finished => Ok(Event::Finished),
            AggregatorTransform::Generate(_) => self.generate_event(),
            AggregatorTransform::ConsumeData(_) => self.consume_event(),
        }
    }

    fn process(&mut self) -> Result<()> {
        match self {
            AggregatorTransform::Finished => Ok(()),
            AggregatorTransform::ConsumeData(state) => state.consume(),
            AggregatorTransform::Generate(state) => state.generate(),
        }
    }
}

impl<TAggregator: Aggregator + 'static> AggregatorTransform<TAggregator> {
    #[inline(always)]
    fn consume_event(&mut self) -> Result<Event> {
        if let AggregatorTransform::ConsumeData(state) = self {
            if state.input_data_block.is_some() {
                return Ok(Event::Sync);
            }

            if state.input_port.is_finished() {
                let mut temp_state = AggregatorTransform::Finished;
                std::mem::swap(self, &mut temp_state);
                temp_state = temp_state.to_generate()?;
                std::mem::swap(self, &mut temp_state);
                debug_assert!(matches!(temp_state, AggregatorTransform::Finished));
                return Ok(Event::Sync);
            }

            return match state.input_port.has_data() {
                true => {
                    state.input_data_block = Some(state.input_port.pull_data().unwrap()?);
                    Ok(Event::Sync)
                }
                false => {
                    state.input_port.set_need_data();
                    Ok(Event::NeedData)
                }
            };
        }

        Err(ErrorCode::LogicalError("It's a bug"))
    }

    #[inline(always)]
    fn generate_event(&mut self) -> Result<Event> {
        if let AggregatorTransform::Generate(state) = self {
            if state.output_port.is_finished() {
                let mut temp_state = AggregatorTransform::Finished;
                std::mem::swap(self, &mut temp_state);
                return Ok(Event::Finished);
            }

            if !state.output_port.can_push() {
                return Ok(Event::NeedConsume);
            }

            if let Some(block) = state.output_data_block.take() {
                state.output_port.push_data(Ok(block));
                return Ok(Event::NeedConsume);
            }

            if state.is_finished {
                if !state.output_port.is_finished() {
                    state.output_port.finish();
                }

                let mut temp_state = AggregatorTransform::Finished;
                std::mem::swap(self, &mut temp_state);
                return Ok(Event::Finished);
            }

            return Ok(Event::Sync);
        }

        Err(ErrorCode::LogicalError("It's a bug"))
    }
}

struct ConsumeState<TAggregator: Aggregator> {
    inner: TAggregator,
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    input_data_block: Option<DataBlock>,
}

impl<TAggregator: Aggregator> ConsumeState<TAggregator> {
    pub fn consume(&mut self) -> Result<()> {
        if let Some(input_data) = self.input_data_block.take() {
            self.inner.consume(input_data)?;
        }

        Ok(())
    }
}

struct GenerateState<TAggregator: Aggregator> {
    inner: TAggregator,
    is_finished: bool,
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    output_data_block: Option<DataBlock>,
}

impl<TAggregator: Aggregator> GenerateState<TAggregator> {
    pub fn generate(&mut self) -> Result<()> {
        let generate_data = self.inner.generate()?;

        if generate_data.is_none() {
            self.is_finished = true;
        }

        self.output_data_block = generate_data;
        Ok(())
    }
}
