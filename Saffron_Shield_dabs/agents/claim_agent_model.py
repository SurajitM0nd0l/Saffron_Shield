
import mlflow
import pandas as pd

from databricks_langchain import ChatDatabricks
from langchain_core.messages import SystemMessage, HumanMessage


class ClaimsAgentModel(mlflow.pyfunc.PythonModel):

    def load_context(self, context):
        with open(context.artifacts["system_prompt"], "r", encoding="utf-8") as f:
            self.system_prompt = f.read()

        self.llm = ChatDatabricks(
            endpoint="databricks-meta-llama-3-3-70b-instruct",
            temperature=0
        )

    def predict(self, context, model_input, params=None):
        outputs = []

        for question in model_input["question"]:
            response = self.llm.invoke([
                SystemMessage(content=self.system_prompt),
                HumanMessage(content=str(question))
            ])
            outputs.append(response.content)

        return pd.DataFrame({"answer": outputs})
